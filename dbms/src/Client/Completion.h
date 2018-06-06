#ifndef CLICKHOUSE_COMPLETION_H
#define CLICKHOUSE_COMPLETION_H

#include <cstdlib>

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
    void insert(const char *word, const char *remainder);
    Entry * find(const char *word, const char *remainder);
public:
    void add_word(const char *word);
    Entry * find_all(const char *word);
    bool has(const char *word);
    void free();
};

}

#endif //CLICKHOUSE_COMPLETION_H
