#include <readline/readline.h>
#include <cstdlib>
#include "Completion.h"

namespace Completion
{
    static uint hashpjw(const char *arKey, uint nKeyLength)
    {
        uint h = 0;
        uint g;
        uint i;
        for (i = 0; i < nKeyLength; i++) {
            h = (h << 4) + arKey[i];
            if ((g = (h & 0xF0000000))) {
                h = h ^ (g >> 24);
                h = h ^ g;
            }
        }
        return h;
    }

    int init_hash_table(HashTable *ht, size_t size)
    {
        ht->hashFunction = hashpjw;
        ht->tableSize = size;
        ht->buckets = (Bucket **)calloc(size, sizeof(Bucket));
        if (!ht->buckets) {
            ht->initialized = false;
            return HASH_FAILURE;
        }
        ht->initialized = true;
        return HASH_SUCCESS;
    }

    void hash_add_word(HashTable *ht, char *word)
    {
        uint i;
        char *pos = word;
        for (i = 1; *pos; i++, pos++) {
            hash_insert_key(ht, word, i, word);
        };
    }

    int hash_insert_key(HashTable *ht, char *key, uint keyLength, char* word)
    {
        uint hash;
        size_t bucketIndex;
        Bucket *bucket;

        if (keyLength <= 0) {
            return HASH_FAILURE;
        }
        hash = ht->hashFunction(key, keyLength);
        bucketIndex = hash % ht->tableSize;
        bucket = ht->buckets[bucketIndex];
        while (bucket) {
            if ( (bucket->hash == hash) && (bucket->keyLength == keyLength)) {
                if (!memcmp(bucket->key, key, keyLength)) {
                    auto *entry = (HashEntry *) calloc(1, sizeof(HashEntry));
                    if (entry == nullptr) {
                        return HASH_FAILURE;
                    }
                    entry->text = word;
                    entry->next = bucket->entry;
                    bucket->entry = entry;

                    return HASH_SUCCESS;
                }
            }
            bucket = bucket->next;
        }
        bucket = (Bucket *) calloc(1, sizeof(Bucket));
        if (bucket == nullptr) {
            return HASH_FAILURE;
        }
        bucket->key = key;
        bucket->keyLength = keyLength;
        bucket->hash = hash;

        bucket->entry = (HashEntry *) calloc(1, sizeof(HashEntry));
        if (bucket->entry == nullptr) {
            return HASH_FAILURE;
        }

        bucket->entry->text = word;
        bucket->entry->next = nullptr;

        bucket->next = ht->buckets[bucketIndex];

        ht->buckets[bucketIndex] = bucket;

        return HASH_SUCCESS;
    }

    Bucket * hash_find_all_matches(HashTable *ht, const char *word, uint length, uint *res_length)
    {
        Bucket *bucket;
        uint hash;
        size_t bucketIndex;
        hash = ht->hashFunction(word, length);
        bucketIndex = hash % ht->tableSize;
        bucket = ht->buckets[bucketIndex];

        while (bucket) {
            if (
                    (bucket->hash == hash)
                    && (bucket->keyLength == length)
                    && (!memcmp(bucket->key, word, length))
                    ) {
                *res_length = length;
                return bucket;
            }
            bucket = bucket->next;
        }

        *res_length = 0;

        return (Bucket *) nullptr;
    }

    void hash_free(HashTable *ht)
    {
        free(ht->buckets);
    }
}



