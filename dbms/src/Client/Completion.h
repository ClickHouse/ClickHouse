#ifndef CLICKHOUSE_COMPLETION_H
#define CLICKHOUSE_COMPLETION_H

#include <iostream>
#include <cstring>
#include <readline/readline.h>

namespace Completion
{

struct Entry {
    char *text;
    struct Entry *next;
    void free()
    {
        if (next) {
            next->free();
            std::free(next);
        }
        std::free(text);
    }
};

class TSTNode {
private:
    TSTNode *left;
    TSTNode *right;
    TSTNode *middle;
    char token;
    Entry *entry;
    void insert(char *word, char *remainder);
    Entry * find(const char *word, const char *remainder);
public:
    void add_word(char *word);
    Entry * find_all(const char *word);
    void free();
};

}

#endif //CLICKHOUSE_COMPLETION_H
