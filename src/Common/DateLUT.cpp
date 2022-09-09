#include "DateLUT.h"

#include <Poco/DigestStream.h>
#include <Poco/Exception.h>
#include <Poco/SHA1Engine.h>
#include <Common/filesystemHelpers.h>

#include <filesystem>
#include <fstream>


namespace
{

Poco::DigestEngine::Digest calcSHA1(const std::string & path)
{
    std::ifstream stream(path);
    if (!stream)
        throw Poco::Exception("Error while opening file: '" + path + "'.");
    Poco::SHA1Engine digest_engine;
    Poco::DigestInputStream digest_stream(digest_engine, stream);
    digest_stream.ignore(std::numeric_limits<std::streamsize>::max());
    if (!stream.eof())
        throw Poco::Exception("Error while reading file: '" + path + "'.");
    return digest_engine.digest();
}


std::string determineDefaultTimeZone()
{
    namespace fs = std::filesystem;

    const char * tzdir_env_var = std::getenv("TZDIR");
    fs::path tz_database_path = tzdir_env_var ? tzdir_env_var : "/usr/share/zoneinfo/";

    fs::path tz_file_path;
    std::string error_prefix;
    const char * tz_env_var = std::getenv("TZ");

    /// In recent tzdata packages some files now are symlinks and canonical path resolution
    /// may give wrong timezone names - store the name as it is, if possible.
    std::string tz_name;

    if (tz_env_var)
    {
        error_prefix = std::string("Could not determine time zone from TZ variable value: '") + tz_env_var + "': ";

        if (*tz_env_var == ':')
            ++tz_env_var;

        tz_file_path = tz_env_var;
        tz_name = tz_env_var;
    }
    else
    {
        error_prefix = "Could not determine local time zone: ";
        tz_file_path = "/etc/localtime";

        /// No TZ variable and no tzdata installed (e.g. Docker)
        if (!fs::exists(tz_file_path))
            return "UTC";

        /// Read symlink but not transitive.
        /// Example:
        ///  /etc/localtime -> /usr/share/zoneinfo//UTC
        ///  /usr/share/zoneinfo//UTC -> UCT
        /// But the preferred time zone name is pointed by the first link (UTC), and the second link is just an internal detail.
        if (FS::isSymlink(tz_file_path))
        {
            tz_file_path = FS::readSymlink(tz_file_path);
            /// If it's relative - make it absolute.
            if (tz_file_path.is_relative())
                tz_file_path = (fs::path("/etc/") / tz_file_path).lexically_normal();
        }
    }

    try
    {
        tz_database_path = fs::weakly_canonical(tz_database_path);

        /// The tzdata file exists. If it is inside the tz_database_dir,
        /// then the relative path is the time zone id.
        {
            fs::path relative_path = tz_file_path.lexically_relative(tz_database_path);

            if (!relative_path.empty() && *relative_path.begin() != ".." && *relative_path.begin() != ".")
                return tz_name.empty() ? relative_path.string() : tz_name;
        }

        /// Try the same with full symlinks resolution
        {
            if (!tz_file_path.is_absolute())
                tz_file_path = tz_database_path / tz_file_path;

            tz_file_path = fs::weakly_canonical(tz_file_path);

            fs::path relative_path = tz_file_path.lexically_relative(tz_database_path);
            if (!relative_path.empty() && *relative_path.begin() != ".." && *relative_path.begin() != ".")
                return tz_name.empty() ? relative_path.string() : tz_name;
        }

        /// The file is not inside the tz_database_dir, so we hope that it was copied (not symlinked)
        /// and try to find the file with exact same contents in the database.

        size_t tzfile_size = fs::file_size(tz_file_path);
        Poco::SHA1Engine::Digest tzfile_sha1 = calcSHA1(tz_file_path.string());

        fs::recursive_directory_iterator begin(tz_database_path);
        fs::recursive_directory_iterator end;
        for (auto candidate_it = begin; candidate_it != end; ++candidate_it)
        {
            const auto & path = candidate_it->path();
            if (path.filename() == "posix" || path.filename() == "right")
            {
                /// Some timezone databases contain copies of toplevel tzdata files in the posix/ directory
                /// and tzdata files with leap seconds in the right/ directory. Skip them.
                candidate_it.disable_recursion_pending();
                continue;
            }

            if (!fs::is_regular_file(*candidate_it) || path.filename() == "localtime")
                continue;

            if (fs::file_size(path) == tzfile_size && calcSHA1(path.string()) == tzfile_sha1)
                return path.lexically_relative(tz_database_path).string();
        }
    }
    catch (const Poco::Exception & ex)
    {
        throw Poco::Exception(error_prefix + ex.message(), ex);
    }
    catch (const std::exception & ex)
    {
        throw Poco::Exception(error_prefix + ex.what());
    }

    throw Poco::Exception(error_prefix + "custom time zone file used.");
}

}

DateLUT::DateLUT()
{
    /// Initialize the pointer to the default DateLUTImpl.
    std::string default_time_zone = determineDefaultTimeZone();
    default_impl.store(&getImplementation(default_time_zone), std::memory_order_release);
}


const DateLUTImpl & DateLUT::getImplementation(const std::string & time_zone) const
{
    std::lock_guard<std::mutex> lock(mutex);

    auto it = impls.emplace(time_zone, nullptr).first;
    if (!it->second)
        it->second = std::unique_ptr<DateLUTImpl>(new DateLUTImpl(time_zone));

    return *it->second;
}

DateLUT & DateLUT::getInstance()
{
    static DateLUT ret;
    return ret;
}
