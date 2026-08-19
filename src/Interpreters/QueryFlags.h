#pragma once

namespace DB
{

struct QueryFlags
{
    bool internal = false; /// If true, this query is caused by another query and thus needn't be registered in the ProcessList.
    bool distributed_backup_restore = false; /// If true, this query is a part of backup restore.
    bool parse_query_from_initial_buffer = false; /// If true, do not read more data while parsing the query. The remaining input can be streaming insert data.
    /// If true, the HTTP client announced a request body with a non-zero `Content-Length`.
    /// It tells whether external data is on its way in the cases where the body cannot be inspected yet,
    /// which is the case when the HTTP 100 Continue response is deferred.
    bool http_request_has_body = false;
    /// With a chunked transfer encoding, the request headers cannot distinguish an empty body from a
    /// non-empty one. The body has to be inspected after sending a deferred HTTP `100 Continue` response.
    bool http_request_body_is_chunked = false;
    bool background = false; /// If true, this query is the background run scheduled by executeQueryInBackground.
};

}
