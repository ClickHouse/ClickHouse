#include "DateLUT.h"

#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUTImpl.h>
#include <Common/filesystemHelpers.h>
#include <Core/Settings.h>

#include <Poco/DigestStream.h>
#include <Poco/Exception.h>
#include <Poco/SHA1Engine.h>

#include <filesystem>
#include <fstream>

#include <cctz/zone_info_source.h>

/// Embedded timezones.
std::string_view getTimeZone(const char * name);

namespace DB
{
namespace Setting
{
    extern const SettingsTimezone session_timezone;
}
}

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

    const char * tzdir_env_var = std::getenv("TZDIR"); // NOLINT(concurrency-mt-unsafe) // ok, because it does not run concurrently with other getenv calls
    fs::path tz_database_path = tzdir_env_var ? tzdir_env_var : "/usr/share/zoneinfo/";

    fs::path tz_file_path;
    std::string error_prefix;
    const char * tz_env_var = std::getenv("TZ"); // NOLINT(concurrency-mt-unsafe) // ok, because it does not run concurrently with other getenv calls

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

const DateLUTImpl & DateLUT::instance()
{
    const auto & date_lut = getInstance();

    if (DB::CurrentThread::isInitialized())
    {
        std::string timezone_from_context;
        const DB::ContextPtr query_context = DB::CurrentThread::get().getQueryContext();

        if (query_context)
        {
            timezone_from_context = extractTimezoneFromContext(query_context);

            if (!timezone_from_context.empty())
                return date_lut.getImplementation(timezone_from_context);
        }

        /// On the server side, timezone is passed in query_context,
        /// but on CH-client side we have no query context,
        /// and each time we modify client's global context
        const DB::ContextPtr global_context = DB::CurrentThread::get().getGlobalContext();
        if (global_context)
        {
            timezone_from_context = extractTimezoneFromContext(global_context);

            if (!timezone_from_context.empty())
                return date_lut.getImplementation(timezone_from_context);
        }
    }
    return serverTimezoneInstance();
}

DateLUT::DateLUT()
{
    /// Initialize the pointer to the default DateLUTImpl.
    std::string default_time_zone = determineDefaultTimeZone();
    default_impl.store(&getImplementation(default_time_zone), std::memory_order_release);
}


const DateLUTImpl & DateLUT::getImplementation(const std::string & time_zone) const
{
    std::lock_guard lock(mutex);

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

std::string DateLUT::extractTimezoneFromContext(DB::ContextPtr query_context)
{
    return query_context->getSettingsRef()[DB::Setting::session_timezone].value;
}

/// By default prefer to load timezones from blobs linked to the binary.
/// The blobs are provided by "tzdata" library.
/// This allows to avoid dependency on system tzdata.
namespace cctz_extension
{
    namespace
    {
        class Source : public cctz::ZoneInfoSource
        {
        public:
            Source(const char * data_, size_t size_) : data(data_), size(size_) { }

            size_t Read(void * buf, size_t bytes) override
            {
                bytes = std::min(bytes, size);
                memcpy(buf, data, bytes);
                data += bytes;
                size -= bytes;
                return bytes;
            }

            int Skip(size_t offset) override
            {
                if (offset <= size)
                {
                    data += offset;
                    size -= offset;
                    return 0;
                }
                else
                {
                    errno = EINVAL;
                    return -1;
                }
            }

        private:
            const char * data;
            size_t size;
        };

        std::unique_ptr<cctz::ZoneInfoSource>
        custom_factory(const std::string & name, const std::function<std::unique_ptr<cctz::ZoneInfoSource>(const std::string & name)> & fallback)
        {
            std::string_view tz_file = getTimeZone(name.data());

            if (!tz_file.empty())
                return std::make_unique<Source>(tz_file.data(), tz_file.size());

            return fallback(name);
        }
    }
    cctz_extension::ZoneInfoSourceFactory zone_info_source_factory = custom_factory;
}

/// If `prefer_system_tzdata` is turned on in config, redefine tzdata lookup order:
/// First, try to use system tzdata, then use built-in.
void DateLUT::setPreferSystemTZData()
{
    cctz_extension::zone_info_source_factory = [] (
                                                   const std::string & name,
                                                   const std::function<std::unique_ptr<cctz::ZoneInfoSource>(const std::string & name)> & fallback
                                                   ) -> std::unique_ptr<cctz::ZoneInfoSource>
    {
	    auto system_tz_source = fallback(name);
        if (system_tz_source)
            return system_tz_source;

        std::string_view tz_file = ::getTimeZone(name.data());

        if (!tz_file.empty())
            return std::make_unique<cctz_extension::Source>(tz_file.data(), tz_file.size());

        /// If not found in system AND in built-in, let fallback() handle this.
        return system_tz_source;
    };
}
