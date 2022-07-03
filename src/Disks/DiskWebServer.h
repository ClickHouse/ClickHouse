#pragma once

#include <Disks/IDiskRemote.h>
#include <IO/WriteBufferFromFile.h>
#include <Core/UUID.h>
#include <set>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/*
 * Quick ready test: ATTACH TABLE test_hits UUID '1ae36516-d62d-4218-9ae3-6516d62da218' ( WatchID UInt64, JavaEnable UInt8, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID UInt32, ClientIP UInt32, ClientIP6 FixedString(16), RegionID UInt32, UserID UInt64, CounterClass Int8, OS UInt8, UserAgent UInt8, URL String, Referer String, URLDomain String, RefererDomain String, Refresh UInt8, IsRobot UInt8, RefererCategories Array(UInt16), URLCategories Array(UInt16), URLRegions Array(UInt32), RefererRegions Array(UInt32), ResolutionWidth UInt16, ResolutionHeight UInt16, ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8, FlashMinor2 String, NetMajor UInt8, NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2), CookieEnable UInt8, JavascriptEnable UInt8, IsMobile UInt8, MobilePhone UInt8, MobilePhoneModel String, Params String, IPNetworkID UInt32, TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String, AdvEngineID UInt8, IsArtifical UInt8, WindowClientWidth UInt16, WindowClientHeight UInt16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 UInt8, SilverlightVersion2 UInt8, SilverlightVersion3 UInt32, SilverlightVersion4 UInt16, PageCharset String, CodeVersion UInt32, IsLink UInt8, IsDownload UInt8, IsNotBounce UInt8, FUniqID UInt64, HID UInt32, IsOldCounter UInt8, IsEvent UInt8, IsParameter UInt8, DontCountHits UInt8, WithHash UInt8, HitColor FixedString(1), UTCEventTime DateTime, Age UInt8, Sex UInt8, Income UInt8, Interests UInt16, Robotness UInt8, GeneralInterests Array(UInt16), RemoteIP UInt32, RemoteIP6 FixedString(16), WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage FixedString(2), BrowserCountry FixedString(2), SocialNetwork String, SocialAction String, HTTPError UInt16, SendTiming Int32, DNSTiming Int32, ConnectTiming Int32, ResponseStartTiming Int32, ResponseEndTiming Int32, FetchTiming Int32, RedirectTiming Int32, DOMInteractiveTiming Int32, DOMContentLoadedTiming Int32, DOMCompleteTiming Int32, LoadEventStartTiming Int32, LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32, FirstPaintTiming Int32, RedirectCount Int8, SocialSourceNetworkID UInt8, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3), ParamCurrencyID UInt16, GoalsReached Array(UInt32), OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID UInt8, RefererHash UInt64, URLHash UInt64, CLID UInt32, YCLID UInt64, ShareService String, ShareURL String, ShareTitle String, ParsedParams Nested(Key1 String, Key2 String, Key3 String, Key4 String, Key5 String, ValueDouble Float64), IslandID FixedString(16), RequestNum UInt32, RequestTry UInt8) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS storage_policy='web';
 *
 *   <storage_configuration>
 *       <disks>
 *           <web>
 *               <type>web</type>
 *               <endpoint>https://clickhouse-datasets.s3.amazonaws.com/disk-with-static-files-tests/test-hits/</endpoint>
 *           </web>
 *       </disks>
 *       <policies>
 *           <web>
 *               <volumes>
 *                   <main>
 *                       <disk>web</disk>
 *                   </main>
 *               </volumes>
 *           </web>
 *       </policies>
 *   </storage_configuration>
 *
 * If query fails with `DB:Exception Unreachable URL` -- may help to adjust settings: http_connection_timeout, http_receive_timeout, keep_alive_timeout.
 *
 * To get files for upload run:
 * clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir>
 * (--metadata-path can be found in query: `select data_paths from system.tables where name='<table_name>';`) /// NOLINT
 *
 * When loading files by <endpoint> they must be loaded into <endpoint>/store/ path, but config must conrain only <endpoint>.
 *
 * If url is not reachable on disk load when server is starting up tables, then all errors are caught.
 * If in this case there were errors, tables can be reloaded (become visible) via detach table table_name -> attach table table_name.
 * If metadata was successfully loaded at server startup, then tables are available straight away.
**/
class DiskWebServer : public IDisk, WithContext
{

public:
    DiskWebServer(const String & disk_name_,
                  const String & url_,
                  ContextPtr context,
                  size_t min_bytes_for_seek_);

    bool supportZeroCopyReplication() const override { return false; }

    DiskType getType() const override { return DiskType::WebServer; }

    bool isRemote() const override { return true; }

    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & path,
                                                     const ReadSettings & settings,
                                                     std::optional<size_t> read_hint,
                                                     std::optional<size_t> file_size) const override;

    /// Disk info

    const String & getName() const final override { return name; }

    const String & getPath() const final override { return url; }

    bool isReadOnly() const override { return true; }

    UInt64 getTotalSpace() const final override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const final override { return std::numeric_limits<UInt64>::max(); }
    UInt64 getUnreservedSpace() const final override { return std::numeric_limits<UInt64>::max(); }

    /// Read-only part

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    size_t getFileSize(const String & path) const override;

    void listFiles(const String & /* path */, std::vector<String> & /* file_names */) override { }

    void setReadOnly(const String & /* path */) override {}

    bool isDirectory(const String & path) const override;

    DiskDirectoryIteratorPtr iterateDirectory(const String & /* path */) override;

    Poco::Timestamp getLastModified(const String &) override { return Poco::Timestamp{}; }

    /// Write and modification part

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String &, size_t, WriteMode, const WriteSettings &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void moveFile(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void replaceFile(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeFile(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeFileIfExists(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    ReservationPtr reserve(UInt64 /*bytes*/) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeRecursive(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeSharedFile(const String &, bool) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeSharedRecursive(const String &, bool) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void clearDirectory(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void moveDirectory(const String &, const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void removeDirectory(const String &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    void setLastModified(const String &, const Poco::Timestamp &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
    }

    std::vector<String> getRemotePaths(const String &) const override { return {}; }

    void getRemotePathsRecursive(const String &, std::vector<LocalPathWithRemotePaths> &) override {}

    /// Create part

    void createFile(const String &) final override {}

    void createDirectory(const String &) override {}

    void createDirectories(const String &) override {}

    void createHardLink(const String &, const String &) override {}

private:
    void initialize(const String & uri_path) const;

    enum class FileType
    {
        File,
        Directory
    };

    struct FileData
    {
        FileType type{};
        size_t size = 0;
    };

    using Files = std::unordered_map<String, FileData>; /// file path -> file data
    mutable Files files;

    Poco::Logger * log;
    String url;
    String name;

    size_t min_bytes_for_seek;
};

}
