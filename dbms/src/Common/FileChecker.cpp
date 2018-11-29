#include <common/JSON.h>
#include <Poco/Path.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/escapeForFileName.h>

#include <Common/FileChecker.h>


namespace DB
{


FileChecker::FileChecker(const std::string & file_info_path_)
{
    setPath(file_info_path_);
}

void FileChecker::setPath(const std::string & file_info_path_)
{
    files_info_path = file_info_path_;

    Poco::Path path(files_info_path);
    tmp_files_info_path = path.parent().toString() + "tmp_" + path.getFileName();
}

void FileChecker::update(const Poco::File & file)
{
    initialize();
    updateImpl(file);
    save();
}

void FileChecker::update(const Files::const_iterator & begin, const Files::const_iterator & end)
{
    initialize();
    for (auto it = begin; it != end; ++it)
        updateImpl(*it);
    save();
}

bool FileChecker::check() const
{
    /** Read the files again every time you call `check` - so as not to violate the constancy.
        * `check` method is rarely called.
        */
    Map local_map;
    load(local_map, files_info_path);

    if (local_map.empty())
        return true;

    for (const auto & name_size : local_map)
    {
        Poco::File file(Poco::Path(files_info_path).parent().toString() + "/" + name_size.first);
        if (!file.exists())
        {
            LOG_ERROR(log, "File " << file.path() << " doesn't exist");
            return false;
        }

        size_t real_size = file.getSize();
        if (real_size != name_size.second)
        {
            LOG_ERROR(log, "Size of " << file.path() << " is wrong. Size is " << real_size << " but should be " << name_size.second);
            return false;
        }
    }

    return true;
}

void FileChecker::initialize()
{
    if (initialized)
        return;

    load(map, files_info_path);
    initialized = true;
}

void FileChecker::updateImpl(const Poco::File & file)
{
    map[Poco::Path(file.path()).getFileName()] = file.getSize();
}

void FileChecker::save() const
{
    {
        WriteBufferFromFile out(tmp_files_info_path);

        /// So complex JSON structure - for compatibility with the old format.
        writeCString("{\"yandex\":{", out);

        auto settings = FormatSettings();
        for (auto it = map.begin(); it != map.end(); ++it)
        {
            if (it != map.begin())
                writeString(",", out);

            /// `escapeForFileName` is not really needed. But it is left for compatibility with the old code.
            writeJSONString(escapeForFileName(it->first), out, settings);
            writeString(":{\"size\":\"", out);
            writeIntText(it->second, out);
            writeString("\"}", out);
        }

        writeCString("}}", out);
        out.next();
    }

    Poco::File current_file(files_info_path);

    if (current_file.exists())
    {
        std::string old_file_name = files_info_path + ".old";
        current_file.renameTo(old_file_name);
        Poco::File(tmp_files_info_path).renameTo(files_info_path);
        Poco::File(old_file_name).remove();
    }
    else
        Poco::File(tmp_files_info_path).renameTo(files_info_path);
}

void FileChecker::load(Map & local_map, const std::string & path)
{
    local_map.clear();

    if (!Poco::File(path).exists())
        return;

    ReadBufferFromFile in(path);
    WriteBufferFromOwnString out;

    /// The JSON library does not support whitespace. We delete them. Inefficient.
    while (!in.eof())
    {
        char c;
        readChar(c, in);
        if (!isspace(c))
            writeChar(c, out);
    }
    JSON json(out.str());

    JSON files = json["yandex"];
    for (const auto & name_value : files)
        local_map[unescapeForFileName(name_value.getName())] = name_value.getValue()["size"].toUInt();
}

}
