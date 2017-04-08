#pragma once

#include <Dictionaries/IDictionary.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/randomSeed.h>
#include <common/MultiVersion.h>
#include <common/logger_useful.h>
#include <Poco/Event.h>
#include <unistd.h>
#include <time.h>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <chrono>
#include <random>

namespace DB
{

class Context;

/** Manages user-defined dictionaries.
*    Monitors configuration file and automatically reloads dictionaries in a separate thread.
*    The monitoring thread wakes up every @check_period_sec seconds and checks
*    modification time of dictionaries' configuration file. If said time is greater than
*    @config_last_modified, the dictionaries are created from scratch using configuration file,
*    possibly overriding currently existing dictionaries with the same name (previous versions of
*    overridden dictionaries will live as long as there are any users retaining them).
*
*    Apart from checking configuration file for modifications, each non-cached dictionary
*    has a lifetime of its own and may be updated if it's source reports that it has been
*    modified. The time of next update is calculated by choosing uniformly a random number
*    distributed between lifetime.min_sec and lifetime.max_sec.
*    If either of lifetime.min_sec and lifetime.max_sec is zero, such dictionary is never updated.
*/
class ExternalDictionaries
{
private:
    friend class StorageSystemDictionaries;

    mutable std::mutex dictionaries_mutex;

    using DictionaryPtr = std::shared_ptr<MultiVersion<IDictionaryBase>>;
    struct DictionaryInfo final
    {
        DictionaryPtr dict;
        std::string origin;
        std::exception_ptr exception;
    };

    struct FailedDictionaryInfo final
    {
        std::unique_ptr<IDictionaryBase> dict;
        std::chrono::system_clock::time_point next_attempt_time;
        UInt64 error_count;
    };

    /** name -> dictionary.
      */
    std::unordered_map<std::string, DictionaryInfo> dictionaries;

    /** Here are dictionaries, that has been never loaded sussessfully.
      * They are also in 'dictionaries', but with nullptr as 'dict'.
      */
    std::unordered_map<std::string, FailedDictionaryInfo> failed_dictionaries;

    /** Both for dictionaries and failed_dictionaries.
      */
    std::unordered_map<std::string, std::chrono::system_clock::time_point> update_times;

    std::mt19937_64 rnd_engine{randomSeed()};

    Context & context;

    std::thread reloading_thread;
    Poco::Event destroy;

    Logger * log;

    std::unordered_map<std::string, Poco::Timestamp> last_modification_times;

    void reloadImpl(bool throw_on_error = false);
    void reloadFromFile(const std::string & config_path, bool throw_on_error);

    void reloadPeriodically();

public:
    /// Dictionaries will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    ExternalDictionaries(Context & context, const bool throw_on_error);
    ~ExternalDictionaries();

    MultiVersion<IDictionaryBase>::Version getDictionary(const std::string & name) const;
};

}
