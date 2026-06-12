#include <Interpreters/Context.h>
#include <ICommand.h>

#include <IO/copyData.h>
#include <IO/ReadHelpers.h>
#include <Common/ShellCommand.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Disks/WriteMode.h>

#include <boost/algorithm/string/trim.hpp>

#include <thread>

#include <signal.h>
#include <Common/ErrnoException.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandSed final : public ICommand
{
public:
    CommandSed() : ICommand("CommandSed")
    {
        command_name = "sed";
        /// Only a single `sed` expression with no options is supported: the expression is passed
        /// to `sed` as one argument. Multiple expressions (`-e ... -e ...`) or per-expression
        /// options (e.g. `-n` together with an address like `4,10p`) are not supported.
        description = "Apply a single `sed` expression to a file on the current disk, in place";
        options_description.add_options()(
            "expression", po::value<String>(), "sed expression to apply (mandatory, positional)")(
            "path", po::value<String>(), "file to edit in place (mandatory, positional)");
        positional_options_description.add("expression", 1);
        positional_options_description.add("path", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        const auto & disk = client.getCurrentDiskWithPath();
        String expression = getValueFromCommandLineOptionsThrow<String>(options, "expression");
        String path = disk.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path"));

        if (disk.getDisk()->existsDirectory(path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cannot edit '{}': Is a directory", path);
        if (!disk.getDisk()->existsFile(path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path {} on disk {} doesn't exist", path, disk.getDisk()->getName());

        /// Write `sed`'s output to a sibling temp file, then atomically replace the original.
        /// This avoids buffering the whole file in memory and never reads and rewrites the
        /// same file concurrently: the original is untouched until the final swap. On any
        /// failure the temp file is removed and the original is left intact.
        String temp_path = path + ".sed.tmp";
        if (disk.getDisk()->existsFile(temp_path) || disk.getDisk()->existsDirectory(temp_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Temporary file {} on disk {} already exists", temp_path, disk.getDisk()->getName());

        LOG_INFO(
            log,
            "Applying sed expression '{}' to file '{}' at disk '{}'",
            expression,
            path,
            disk.getDisk()->getName());

            SCOPE_EXIT_SAFE(disk.getDisk()->removeFileIfExists(temp_path));

            auto in = disk.getDisk()->readFile(path, getReadSettings());
            auto out = disk.getDisk()->writeFile(temp_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
            SCOPE_EXIT_SAFE(out->cancel());

            /// Start `sed` only after both disk buffers are open. If opening the source or temp file
            /// throws, the child is not yet running, so unwinding cannot leave a `sed` process with an
            /// open `stdin` to be reaped by the blocking destructor -- which would otherwise hang.
            /// Pass the expression as a separate argument so it is never interpreted by the
            /// shell. `sed` reads its input from `stdin` and writes the result to `stdout`.
            ShellCommand::Config config{R"(exec sed -- "$0")"};
            config.arguments.push_back(expression);
            auto command = ShellCommand::execute(config);

            /// Feed the file into `sed`'s `stdin` from a separate thread while we read its
            /// `stdout` here, to avoid a deadlock when the data exceeds the pipe buffer.
            std::exception_ptr feeder_exception;
            std::thread feeder(
                [&]
                {
                    /// On any exit, close sed's stdin so sed sees EOF and the main thread
                    /// cannot block forever reading sed's stdout. cancel() first so close()
                    /// does not flush: if finalize() threw inside close(), the ::close(fd)
                    /// would be skipped and the fd would leak, reintroducing the hang. It is
                    /// a no-op when the close() in the happy path already finalized the buffer.
                    SCOPE_EXIT_SAFE({ command->in.cancel(); command->in.close(); });
                    try
                    {
                        copyData(*in, command->in);
                        command->in.close();
                    }
                    catch (...)
                    {
                        feeder_exception = std::current_exception();
                    }
                });

            try
            {
                copyData(command->out, *out);
            }
            catch (...)
            {
                /// The feeder may be blocked in write(): we are no longer draining sed's
                /// stdout, so sed's pipes fill up and it stops reading stdin. Terminate sed
                /// to break the pipes; the feeder then fails with a broken pipe, runs its
                /// scope guard and exits, making join() safe. The child has not been reaped
                /// yet, so the pid cannot have been reused.
                if (0 != ::kill(command->getPid(), SIGTERM))
                    LOG_WARNING(log, "Cannot send SIGTERM to sed (pid {}): {}", command->getPid(), errnoToString());
                feeder.join();
                throw;
            }
            feeder.join();

            /// Drain `sed`'s stderr before reaping it: `wait` closes the stderr pipe, so it must
            /// be read first. `sed`'s stdout already hit EOF above, meaning `sed` has exited and
            /// its stderr write end is closed, so this read sees EOF and cannot block. `sed` errors
            /// are tiny, so buffering the whole output is fine.
            String stderr_content;
            readStringUntilEOF(stderr_content, command->err);
            boost::trim(stderr_content);

            /// Check `sed`'s exit status before the feeder error. If `sed` exits early (e.g. an
            /// invalid expression), the feeder thread sees a broken pipe while writing to `sed`'s
            /// stdin -- but that is only a symptom, so `sed`'s own diagnostic must take precedence.
            /// Throws if `sed` exited with a non-zero code.
            try
            {
                command->wait();
            }
            catch (Exception & e)
            {
                if (!stderr_content.empty())
                    e.addMessage("sed wrote to stderr: {}", stderr_content);
                throw;
            }

            /// `sed` exited successfully, so any feeder error (e.g. a disk read failure) is real.
            if (feeder_exception)
                std::rethrow_exception(feeder_exception);

            out->finalize();

            disk.getDisk()->replaceFile(temp_path, path);

    }
};

CommandPtr makeCommandSed()
{
    return std::make_shared<DB::CommandSed>();
}

}
