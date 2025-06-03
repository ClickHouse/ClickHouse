#include <string>

#include <IO/parseDateTimeBestEffort.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>


using namespace DB;

int main(int, char **)
try
{
    const DateLUTImpl & local_time_zone = DateLUT::instance();
    const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    time_t res;
    parseDateTimeBestEffort(res, in, local_time_zone, utc_time_zone);
    writeDateTimeText(res, out);
    writeChar('\n', out);

    return 0;
}
catch (const Exception &)
{
    std::cerr << getCurrentExceptionMessage(true) << std::endl;
    return 1;
}
