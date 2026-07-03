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

    /// Whether the initial history load has finished (or was skipped via `markLoaded`). Predictions
    /// are only served afterwards, so the model is not queried while it is still being seeded.
    bool isLoaded() const { return loading_finished.load(); }

    /// Predict the next tokens for `prefix` (the text up to the cursor), most likely first. Returns
    /// an empty list until the model is loaded or when there is nothing to predict.
    std::vector<std::string> predictNextTokens(const String & prefix);

    void addQuery(const String & query);

    /// Mark the model as ready without seeding it from server history (e.g. clickhouse-local, where
    /// there is no persistent `system.query_log`). The model still learns from queries entered this
    /// session; without this the `loading_finished` flag would stay `false` forever and every
    /// prediction request would return an empty result.
    void markLoaded() { loading_finished = true; }

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

    std::vector<std::string> history_queries TSA_GUARDED_BY(mutex);

    std::atomic<bool> loading_finished = false;

    std::thread loading_thread;

    std::mutex mutex;

    std::atomic<int> last_error{-1};

    size_t query_history_limit = 700;

    /// `log_queries = 0`: this seeding query is itself an initial query by the current user, so
    /// without disabling logging it would be written to `system.query_log` and then re-read (and
    /// used to train the model) on the next session — a client helper query masquerading as
    /// user-entered SQL. Access to `system.query_log` may be denied; `load` handles that quietly.
    const String history_query = fmt::format(
        "SELECT query FROM (SELECT query, query_start_time FROM system.query_log WHERE is_initial_query = 1 AND "
        "type = 2 AND user IN (SELECT currentUser()) ORDER BY event_date DESC, event_time DESC LIMIT {}) AS recent_queries "
        "ORDER BY query_start_time ASC SETTINGS log_queries = 0;",
        query_history_limit);
};
}
