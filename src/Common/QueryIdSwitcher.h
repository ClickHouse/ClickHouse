#pragma once

#include <string>
#include <boost/core/noncopyable.hpp>

namespace DB
{

/**
 * RAII wrapper that temporarily replaces the query id of the current thread.
 *
 * On construction it remembers the current query id (if any) and sets the thread's
 * query id to {old-id}::{new-id} (or just {new-id} when there was no query id).
 * On destruction it restores the original query id.
 *
 * Useful for attributing background work (e.g. table startup) to a recognizable
 * query id in traces and logs, while keeping any enclosing query id as a prefix.
 *
 * Does nothing if the current thread is not initialized.
 */
class QueryIdSwitcher : private boost::noncopyable
{
public:
    explicit QueryIdSwitcher(const std::string & new_query_id);
    ~QueryIdSwitcher();

private:
    bool switched = false;
    std::string prev_query_id;
};

}
