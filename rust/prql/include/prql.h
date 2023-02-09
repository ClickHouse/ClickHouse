#pragma once

extern "C" {

    int to_json(const char * query, char * out);

    int to_sql(const char * query, char * out);

} // extern "C"
