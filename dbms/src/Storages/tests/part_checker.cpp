#include <Poco/ConsoleChannel.h>
#include <Poco/DirectoryIterator.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Common/Exception.h>

using namespace DB;

Poco::Path getMarksFile(const std::string & part_path)
{
    Poco::DirectoryIterator it(part_path);
    Poco::DirectoryIterator end;
    while (it != end)
    {
        Poco::Path p(it.path());
        auto extension = p.getExtension();
        if (extension == "mrk2" || extension == "mrk")
            return p;
        ++it;
    }
    throw Exception("Cannot find any mark file in directory " + part_path, DB::ErrorCodes::METRIKA_OTHER_ERROR);
}

MergeTreeIndexGranularity readGranularity(const Poco::Path & mrk_file_path, size_t fixed_granularity)
{

    MergeTreeIndexGranularity result;
    auto extension = mrk_file_path.getExtension();

    DB::ReadBufferFromFile mrk_in(mrk_file_path.toString());

    for (size_t mark_num = 0; !mrk_in.eof(); ++mark_num)
    {
        UInt64 offset_in_compressed_file = 0;
        UInt64 offset_in_decompressed_block = 0;
        DB::readBinary(offset_in_compressed_file, mrk_in);
        DB::readBinary(offset_in_decompressed_block, mrk_in);
        UInt64 index_granularity_rows = 0;
        if (extension == "mrk2")
            DB::readBinary(index_granularity_rows, mrk_in);
        else
            index_granularity_rows = fixed_granularity;
        result.appendMark(index_granularity_rows);
    }
    return result;
}

int main(int argc, char ** argv)
{

    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Logger::root().setChannel(channel);
    Logger::root().setLevel("trace");

    if (argc != 4)
    {
        std::cerr << "Usage: " << argv[0] << " path strict index_granularity" << std::endl;
        return 1;
    }

    try
    {
        std::string full_path{argv[1]};

        auto mrk_file_path = getMarksFile(full_path);
        size_t fixed_granularity{parse<size_t>(argv[3])};
        auto adaptive_granularity = readGranularity(mrk_file_path, fixed_granularity);
        auto marks_file_extension = "." + mrk_file_path.getExtension();
        bool require_checksums = parse<bool>(argv[2]);

        checkDataPart(full_path, adaptive_granularity, marks_file_extension, require_checksums, {});
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    return 0;
}
