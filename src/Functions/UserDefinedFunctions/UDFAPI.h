#ifndef CLICKHOUSE_UDFAPI_H
#define CLICKHOUSE_UDFAPI_H

#include <unistd.h>

struct UserDefinedFunctionResult {
    char * data_type;
    void * data;
    size_t size;
};

typedef struct UserDefinedFunctionResult (*ExecImpl)(UserDefinedFunctionResult* input);

struct UserDefinedFunction {
    /// Name of UDF
    char * name;
    /// Accept null terminated list of input columns
    /// Returns one column
    ExecImpl exec_impl;
};

struct UDFList {
    /// Name of UDF lib
    char * name;
    /// Null terminated list
    UserDefinedFunction * functions;
};

typedef struct UDFList (*UDFInit)();
typedef void (*UDFListFree)(struct UDFList);

#endif //CLICKHOUSE_UDFAPI_H
