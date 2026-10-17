#pragma once

#include <atomic>
#include <thread>
#include <vector>
#include <Client/Connection.h>
#include <Client/ConnectionParameters.h>
#include <Client/IServerConnection.h>
#include <Client/LocalConnection.h>
#include <Columns/ColumnString.h>
#include <IO/ConnectionTimeouts.h>
#include <Parsers/Lexer.h>
#include <Client/AutocompleteModel.h>


namespace DB
{

/// Predictive autocomplete for the interactive client: seeds a Markov model from the user's recent
/// query history (loaded in the background) and updates it with every query entered this session,
/// then predicts the next tokens for the line being typed. It is a separate, opt-in source layered
/// on top of the static `Suggest` dictionary that drives Tab completion and inline hints.
class Autocomplete : public boost::noncopyable
{
public:
    Autocomplete() = default;

    ~Autocomplete()
    {
        if (loading_thread.joinable())
            loading_thread.join();
    }

    int getLastError() const { return last_error.load(); }

    /// Whether the initial history load has finished. Predictions are only served afterwards, so
    /// the model is not queried while it is still being seeded.
    bool isLoaded() const { return loading_finished.load(); }

    /// Predict the next tokens for `prefix` (the text up to the cursor), most likely first. Returns
    /// an empty list until the model is loaded or when there is nothing to predict.
    std::vector<std::string> predictNextTokens(const String & prefix);

    void addQuery(const String & query);

    void
    fetch(IServerConnection & connection, const ConnectionTimeouts & timeouts, const std::string & query, const ClientInfo & client_info);

    template <typename ConnectionType>
    void load(ContextPtr context, const ConnectionParameters & connection_parameters);
    void load(IServerConnection & connection, const ConnectionTimeouts & timeouts, const ClientInfo & client_info);

    void fillQueriesFromBlock(const Block & block);

private:
    /// Adds a single query to the model. `mutex` must be held: the background history loader and
    /// foreground queries (`ClientBase` calls `addQuery` after every successful query) both mutate
    /// `model`, whose Markov maps are not thread-safe.
    void addQueryToModel(const String & query) TSA_REQUIRES(mutex);

    AutocompleteModel model TSA_GUARDED_BY(mutex) = AutocompleteModel();

    std::atomic<bool> loading_finished = false;

    std::thread loading_thread;

    std::mutex mutex;

    std::atomic<int> last_error{-1};

    size_t query_history_limit = 700;

    /// The history is read from `system.user_query_log`: it shows only the current user's records
    /// of the query log (matching them by the initiating user, which is also correct for queries
    /// that arrived through an initiator), and reading it requires no grants, so any user can seed
    /// the model from their own history. Servers without `system.user_query_log` (older versions,
    /// or `query_log.enable_user_query_log = 0`) make this query fail with `UNKNOWN_TABLE`; `load`
    /// handles that quietly.
    ///
    /// This seeding query is itself an initial query by the current user, so it is written to
    /// the query log and would then be re-read (and used to train the model) on the next
    /// session — a client helper query masquerading as user-entered SQL. To avoid that, the query
    /// excludes itself via the `query NOT LIKE '%...%'` filter below: the filter's pattern literal
    /// contains the marker string, so this query's own text matches the pattern and is filtered out
    /// when it is later found in the log. We deliberately do not use `SETTINGS log_queries = 0` for
    /// this: readonly users cannot modify any setting, and that would make the whole query fail
    /// (`READONLY`), breaking autocomplete for them.
    ///
    /// The suggestion-dictionary query that `Suggest` runs on every interactive session start is
    /// another client helper query that is logged as a normal query of the user. One copy of it is
    /// written per session, so in the history of an interactive-client user it is the single most
    /// repeated query by far, and training on it badly skews the model (e.g. towards `DISTINCT`
    /// right after `SELECT`). It carries no marker that could identify it, so it is excluded by its
    /// distinctive fixed prefix.
    ///
    /// The newest `query_history_limit` queries are selected, but returned oldest-first: the model
    /// weights n-gram counts by an internal timestamp that grows with every added query, so the
    /// newest query must be added last to end up with the largest recency weight.
    const String history_query = fmt::format(
        "SELECT query FROM (SELECT query, query_start_time FROM system.user_query_log WHERE is_initial_query = 1 AND "
        "type = 2 AND query NOT LIKE '%-- clickhouse-client autocomplete history seed%' "
        "AND query NOT LIKE 'SELECT DISTINCT arrayJoin(extractAll(%' "
        "ORDER BY event_date DESC, event_time DESC LIMIT {}) AS recent_queries "
        "ORDER BY query_start_time ASC;",
        query_history_limit);
};
}
