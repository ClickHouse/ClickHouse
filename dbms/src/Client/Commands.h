#ifndef CLICKHOUSE_COMMANDS_H
#define CLICKHOUSE_COMMANDS_H

typedef struct {
    char *name;			/* User printable name of the function. */
} COMMAND;

COMMAND commands[] = {
        {(char *)"SELECT"},
        {(char *)"SELECD"},
        {(char *)"SELECD.sss"},
        {(char *)"INSERT"},
        {(char *)"DELETE"},
        {(char *)nullptr},
};

#endif //CLICKHOUSE_COMMANDS_H
