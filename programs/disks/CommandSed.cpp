#include <ICommand.h>
#include <Interpreters/Context.h>

#include <Disks/WriteMode.h>
#include <IO/ReadHelpers.h>
#include <IO/copyData.h>
#include <Common/ShellCommand.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>

#include <boost/algorithm/string/trim.hpp>

#include <thread>

#include <csignal>
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
    CommandSed()
        : ICommand("CommandSed")
    {
        command_name = "sed";
        // Only supports single sed expressions.
        // Multiple expressions or per-expressions options are not supported.
        description = "Apply a single `sed` expression to a file on the current disk, in place";
        options_description.add_options()("expression", po::value<String>(), "sed expression to apply (mandatory, positional)")(
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

        /// Write sed's output to a temp file. Later, it will replace the original.
        String temp_path = path + ".sed.tmp";
        if (disk.getDisk()->existsFile(temp_path) || disk.getDisk()->existsDirectory(temp_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Temporary file {} on disk {} already exists", temp_path, disk.getDisk()->getName());

        LOG_INFO(log, "Applying sed expression '{}' to file '{}' at disk '{}'", expression, path, disk.getDisk()->getName());

        SCOPE_EXIT_SAFE(disk.getDisk()->removeFileIfExists(temp_path));

        auto in = disk.getDisk()->readFile(path, getReadSettings());
        auto out = disk.getDisk()->writeFile(temp_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        SCOPE_EXIT_SAFE(out->cancel());

        // 1. It's important to start sed after the disk buffers are opened. This way,
        // any errors thrown while opening the files do not leave the sed process hanging.
        // 2. The expression is passed as a separate argument for safety -- otherwise, it could
        // be interpreted by the shell.
        ShellCommand::Config config{R"(exec sed -- "$0")"};
        config.terminate_in_destructor_strategy = ShellCommand::DestructorStrategy(true, SIGTERM);
        config.arguments.push_back(expression);
        auto command = ShellCommand::execute(config);

        // Feed the file into sed's stdin from a separate thread from the one that reads
        // stdout.
        std::exception_ptr feeder_exception;
        std::thread feeder(
            [&]
            {
                // On any exit, cancel and close sed's stdin. It is a no-op on the happy path.
                SCOPE_EXIT_SAFE({
                    command->in.cancel();
                    command->in.close();
                });
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
            /// Without SIGTERM, could have the following failure path:
            /// failed write -> sed's stdout undrained -> pipes full -> stdin blocked -> feeder.join() hangs.
            if (0 != ::kill(command->getPid(), SIGTERM))
                LOG_WARNING(log, "Cannot send SIGTERM to sed (pid {}): {}", command->getPid(), errnoToString());
            feeder.join();
            throw;
        }
        feeder.join();

        /// Drain sed's stderr before calling command->wait().
        /// For simplicity, this code assumes stderr output is small and does not need to be concurrently drained.
        /// It could hang if the user intentionally writes a large output to stderr (e.g., sed 'w /dev/stderr' foo.txt).
        String stderr_content;
        readStringUntilEOF(stderr_content, command->err);
        boost::trim(stderr_content);

        /// Check sed's exit status before the feeder error. If sed exits early (e.g. an invalid
        /// expression), the feeder thread sees a broken pipe when writing to stdin. So sed
        /// failures should take prcedence.
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

        if (feeder_exception)
        {
            /// sed exited successfully, but the feeder failed.
            /// This could be a benign case: a valid script that stops reading the
            /// input early (eg, '1q') and causes writes to fail with EPIPE.
            try
            {
                std::rethrow_exception(feeder_exception);
            }
            catch (const ErrnoException & e)
            {
                if (e.getErrno() != EPIPE)
                    throw;
            }
        }

        out->finalize();

        disk.getDisk()->replaceFile(temp_path, path);
    }
};

CommandPtr makeCommandSed()
{
    return std::make_shared<DB::CommandSed>();
}

}
