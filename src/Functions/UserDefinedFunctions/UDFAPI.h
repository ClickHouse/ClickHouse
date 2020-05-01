#ifndef CLICKHOUSE_UDFAPI_H
#define CLICKHOUSE_UDFAPI_H

typedef struct Init {
    char ** function_names;
};

struct Init (* UDFInit)();



#endif //CLICKHOUSE_UDFAPI_H
