#ifndef CLICKHOUSE_COMPLETION_H
#define CLICKHOUSE_COMPLETION_H

#define SUCCESS 0
#define FAILURE 1

#include <sys/types.h>

namespace Completion
{
    struct HashEntry {
        char *text;
        struct HashEntry *next;
    };

    struct Bucket {
        uint hash;
        char *key;
        uint keyLength;
        HashEntry *entry;
        struct Bucket *next;
    };

    struct HashTable {
        bool initialized;
        size_t tableSize;
        uint (*hashFunction)(const char *key, uint keyLength);
        Bucket **buckets;
    };

    int init_hash_table(HashTable *ht, size_t size);
    void hash_add_word(HashTable *ht, char *word);
    int hash_insert_key(HashTable *ht, char *key, uint keyLength, char* word);
    Bucket * hash_find_all_matches(HashTable *ht, const char *word, uint length, uint *res_length);
}

#endif //CLICKHOUSE_COMPLETION_H
