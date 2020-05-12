#ifndef CLICKHOUSE_UDFAPI_H
#define CLICKHOUSE_UDFAPI_H

#include <unistd.h>

struct UserDefinedFunctionResult {
    const char * data_type;
    void * data;
    size_t size;
};

/// All allocated memory will automatically free after ExecImpl return
/// Allocated memory should be used in UserDefinedFunctionResult as data pointer
typedef void * (*AllocateImpl)(size_t size);

/// Return one column of result data
typedef struct UserDefinedFunctionResult (*ExecImpl)(UserDefinedFunctionResult* input, AllocateImpl allocate);
/// Return type of function result (data should be nullptr)
typedef struct UserDefinedFunctionResult (*GetReturnTypeImpl)(UserDefinedFunctionResult* input);

struct UserDefinedFunction {
    /// Name of UDF
    const char * name;

    /// Accept null terminated list of input columns types (data is nullptr)
    /// Returns type of data
    GetReturnTypeImpl get_return_type_impl;

    /// Accept null terminated list of input columns
    /// Returns one column
    ExecImpl exec_impl;
};

struct UDFList {
    /// Name of UDF lib
    const char * name;
    /// Null terminated list
    UserDefinedFunction * functions;
};

typedef struct UDFList (*UDFInit)();
typedef void (*UDFListFree)(struct UDFList);

#endif //CLICKHOUSE_UDFAPI_H
