#pragma once

#include <Common/ZooKeeper/KeeperClientCLI/Commands.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/Names.h>
#include <filesystem>
#include <future>
#include <map>
#include <optional>
#include <unordered_map>
#include <vector>


namespace fs = std::filesystem;

namespace DB
{

static const NameSet four_letter_word_commands
    {
        "ruok", "mntr", "srvr", "stat", "srst", "conf",
        "cons", "crst", "envi", "dirs", "isro", "wchs",
        "wchc", "wchp", "dump", "csnp", "lgif", "rqld",
        "rclc", "clrs", "ftfl", "ydld", "pfev", "lgrq",
        "rcfg", "rcvr", "apiv", "jmst", "jmfp", "jmep",
        "jmdp",
    };

/// Format a ZooKeeper node name for display and round-tripping through the parser.
/// Returns the name bare when it contains no special characters, or wrapped in
/// single quotes with \' and \\ escaping otherwise. The result is always parseable
/// by parseKeeperArg (either as a bare token or as an inline quoted segment).
/// Used by both `ls` output and tab completion.
String formatKeeperNodeName(const String & name);

/// Result of `KeeperClientBase::completeQueryPrefix`.
/// `completions` are texts that replace `prefix[replace_start:]` (same contract as
/// replxx last-word completion). Each entry starts with `prefix[replace_start:]`.
/// `replace_start` is a UTF-8 byte offset into `prefix`
/// (C++ `String` index), not a Unicode code-point or UTF-16 index.
struct KeeperCompletionResult
{
    std::vector<String> completions;
    size_t replace_start = 0;
};

/// Parsed destructive-operation prompts for dashboard confirmation (no execution).
struct KeeperDestructiveConfirmationPreview
{
    std::vector<String> prompts;
    std::optional<String> rejection_reason;
};

class KeeperClientBase
{
public:
    using CommandsMap = std::map<String, Command>;

    explicit KeeperClientBase(std::ostream & cout_, std::ostream & cerr_);

    fs::path getAbsolutePath(const String & relative) const;

    void askConfirmation(const String & prompt, std::function<void()> && callback);

    virtual String executeFourLetterCommand(const String & command);

    /// Process-wide command registry, initialized exactly once (thread-safe).
    static const CommandsMap & getCommands();

    /// Sorted command names plus four-letter words, for completion.
    static const std::vector<String> & getRegisteredCommandNames();

    /// Tab-complete a CLI line prefix (text up to the cursor), shared by
    /// clickhouse-keeper-client and the Keeper HTTP dashboard.
    /// Requires `zookeeper` for path-argument completion.
    KeeperCompletionResult completeQueryPrefix(const String & prefix) const;

    /// Mirror `RMRCommand` / `DeleteStaleBackups` confirmation text on parsed statements.
    KeeperDestructiveConfirmationPreview previewDestructiveConfirmations(const String & text) const;

    zkutil::ZooKeeperPtr zookeeper;
    std::filesystem::path cwd = "/";
    std::function<void()> confirmation_callback;
    bool ask_confirmation = true;

    std::unordered_map<String, std::future<Coordination::WatchResponse>> watches;

    std::ostream & cout;
    std::ostream & cerr;

    void processQueryText(const String & text);

    virtual ~KeeperClientBase() = default;

protected:
    /// Parses and runs `text` with the Keeper token loop (`skip_insignificant=false`)
    /// so completed paths such as `space\ node` round-trip. Throws
    /// `Coordination::Exception` so `clickhouse-keeper-client` can retry hardware errors.
    void executeQueryText(const String & text);

    bool waiting_confirmation = false;
};

}
