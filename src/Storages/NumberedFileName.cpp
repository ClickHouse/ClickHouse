#include <Storages/NumberedFileName.h>

namespace DB
{

std::string addSequenceNumberToFileName(const std::string & path, size_t sequence_number)
{
    /// The name of the file ends at the first dot after the last slash.
    /// When there is no slash (a top-level object key), the whole string is the name: `npos + 1 == 0`.
    size_t pos = path.find_first_of('.', path.find_last_of('/') + 1);
    if (pos == std::string::npos)
        return path + "." + std::to_string(sequence_number);
    return path.substr(0, pos) + "." + std::to_string(sequence_number) + path.substr(pos);
}

NumberedFileNames getNumberedFileNames(const std::string & path)
{
    return {
        .getName = [path](size_t sequence_number) { return addSequenceNumberToFileName(path, sequence_number); },
    };
}

}
