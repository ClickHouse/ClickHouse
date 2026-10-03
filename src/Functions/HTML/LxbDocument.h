#pragma once

#include <lexbor/html/parser.h>

namespace DB
{

/// RAII wrapper for lxb_html_document_t.
struct LxbDocument
{
    lxb_html_document_t * doc;

    LxbDocument() : doc(lxb_html_document_create()) {}
    ~LxbDocument() { if (doc) lxb_html_document_destroy(doc); }

    explicit operator bool() const { return doc != nullptr; }

    LxbDocument(LxbDocument && other) noexcept : doc(other.doc) { other.doc = nullptr; }
    LxbDocument & operator=(LxbDocument && other) noexcept
    {
        if (this != &other)
        {
            if (doc) lxb_html_document_destroy(doc);
            doc = other.doc;
            other.doc = nullptr;
        }
        return *this;
    }

    LxbDocument(const LxbDocument &) = delete;
    LxbDocument & operator=(const LxbDocument &) = delete;
};

}
