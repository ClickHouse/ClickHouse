#include "Completion.h"

namespace Completion
{
    void TSTNode::add_word(char *word)
    {
        insert(word, word);
    }

    void TSTNode::insert(char *word, char *remainder)
    {
        if (!*remainder) {
            return;
        }

        if (!token) {
            token = *remainder;
        }

        if (token > *remainder) {
            if (!left) {
                left = (TSTNode *) calloc(1, sizeof(TSTNode));
            }
            return left->insert(word, remainder);
        }

        if (token < *remainder) {
            if (!right) {
                right = (TSTNode *) calloc(1, sizeof(TSTNode));
            }
            return right->insert(word, remainder);
        }

        auto newEntry = (Entry *) calloc(1, sizeof(Entry));
        newEntry->text = word;
        newEntry->next = entry;
        entry = newEntry;

        if (!middle) {
            middle = (TSTNode *) calloc(1, sizeof(TSTNode));
        }

        return middle->insert(word, ++remainder);
    }

    Entry * TSTNode::find_all(const char *word)
    {
        if (!word) {
            return (Entry *) nullptr;
        }

        return find(word, word);
    }

    Entry * TSTNode::find(const char *word, const char *remainder)
    {
        if (token > *remainder) {
            if (!left) {
                return (Entry *) nullptr;
            }
            return left->find(word, remainder);
        }

        if (token < *remainder) {
            if (!right) {
                return (Entry *) nullptr;
            }
            return right->find(word, remainder);
        }

        if (!middle) {
            return (Entry *) nullptr;
        }

        if (strlen(remainder) == 1) {
            return entry;
        }

        return middle->find(word, ++remainder);
    }

    void TSTNode::free()
    {
        if (left) {
            left->free();
            std::free(left);
        }

        if (right) {
            right->free();
            std::free(right);
        }

        if (middle) {
            middle->free();
            std::free(middle);
        }

        if (entry) {
            entry->free();
            std::free(entry);
        }
    }
}



