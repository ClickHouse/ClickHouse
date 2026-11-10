#include <Common/Coverage.h>

#if defined(SANITIZE_COVERAGE)

#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <vector>

#include <Common/IO.h>
#include <base/coverage.h>

#include <fmt/format.h>

/// Macros to avoid using strlen(), since it may fail if SSE is not supported.
#define writeError(data) do \
    { \
        static_assert(__builtin_constant_p(data)); \
        if (!writeRetry(STDERR_FILENO, data, sizeof(data) - 1)) \
            _Exit(1); \
    } while (false)

__attribute__((no_sanitize("coverage")))
void dumpCoverage()
{
    /// A user can request to dump the coverage information into files at exit.
    /// This is useful for non-server applications such as clickhouse-format or clickhouse-client,
    /// that cannot introspect it with SQL functions at runtime.

    /// The CLICKHOUSE_WRITE_COVERAGE environment variable defines a prefix for a filename 'prefix.pid'
    /// containing the list of addresses of covered .

    /// The format is even simpler than Clang's "sancov": an array of 64-bit addresses, native byte order, no header.

    if (const char * coverage_filename_prefix = getenv("CLICKHOUSE_WRITE_COVERAGE")) // NOLINT(concurrency-mt-unsafe)
    {
        auto dump = [](const std::string & name, auto span)
        {
            /// Write only non-zeros.
            std::vector<uintptr_t> data;
            data.reserve(span.size());
            for (auto addr : span)
                if (addr)
                    data.push_back(addr);

            int fd = ::open(name.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0400);
            if (-1 == fd)
            {
                writeError("Cannot open a file to write the coverage data\n");
            }
            else
            {
                if (!writeRetry(fd, reinterpret_cast<const char *>(data.data()), data.size() * sizeof(data[0])))
                    writeError("Cannot write the coverage data to a file\n");
                if (0 != ::close(fd))
                    writeError("Cannot close the file with coverage data\n");
            }
        };

        dump(fmt::format("{}.{}", coverage_filename_prefix, getpid()), getCumulativeCoverage());
    }
}
#endif

