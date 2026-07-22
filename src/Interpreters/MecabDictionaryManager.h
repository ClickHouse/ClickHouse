#pragma once

#include "config.h"

#if USE_MECAB

#include <base/types.h>

#include <memory>
#include <mutex>

namespace MeCab
{
class Model;
}

namespace DB
{

/// A loaded MeCab dictionary: owns the `MeCab::Model` and the directory it was extracted to.
/// The model is shared; each consumer creates its own `Lattice` from it.
class MecabDictionary
{
public:
    MecabDictionary(std::unique_ptr<MeCab::Model> model_, String dictionary_dir_);
    ~MecabDictionary();

    const MeCab::Model & getModel() const { return *model; }

private:
    std::unique_ptr<MeCab::Model> model;
    String dictionary_dir;
};

using MecabDictionaryPtr = std::shared_ptr<const MecabDictionary>;

/// Loads the Japanese (MeCab) dictionary described under <tokenizer><japanese> in the server config
/// (<dictionaryLocation> + <dictionarySha>). The archive is downloaded once, its SHA-256 verified
/// BEFORE use (mismatch = hard error, loading stops), extracted into a cache keyed by the SHA, and a
/// shared `MeCab::Model` created from it and cached process-wide.
///
/// Fail-closed: missing config, download failure, or checksum mismatch throws; there is no fallback.
class MecabDictionaryManager
{
public:
    static MecabDictionaryManager & instance();

    /// Returns the (lazily loaded) Japanese dictionary. Throws on any error.
    MecabDictionaryPtr getJapaneseDictionary();

private:
    MecabDictionaryPtr loadJapaneseDictionary();

    std::mutex mutex;
    MecabDictionaryPtr cached_dictionary;
    String cached_sha;
};

}

#endif
