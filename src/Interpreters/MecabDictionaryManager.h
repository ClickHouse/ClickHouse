#pragma once

#include "config.h"

#if USE_MECAB

#include <base/types.h>
#include <base/defines.h>

#include <memory>
#include <mutex>

namespace MeCab
{
class Model;
}

namespace DB
{

/// A loaded MeCab dictionary: owns the `MeCab::Model` and its associated dictionary directory.
class MecabDictionary
{
public:
    MecabDictionary(std::unique_ptr<MeCab::Model> model_, String dictionary_path_);

    const MeCab::Model & getModel() const { return *model; }

private:
    const std::unique_ptr<MeCab::Model> model;
    const String dictionary_path;
};

using MecabDictionaryPtr = std::shared_ptr<const MecabDictionary>;

/// Loads the Japanese (MeCab) dictionary described in the server config:
///
///     <tokenizer>
///         <japanese>
///             <dictionary_location>https://.../dictionary.tar.zst</dictionary_location>
///             <dictionary_sha>&lt;hex sha-256 of the archive&gt;</dictionary_sha>
///         </japanese>
///     </tokenizer>
///
/// The archive is downloaded once, its SHA-256 verified before use (mismatch = hard error, loading
/// stops), extracted into a cache keyed by the SHA, and a `MeCab::Model` created from it and cached
/// process-wide. Fail-closed: missing config, download failure, or checksum mismatch throws.
class MecabDictionaryManager
{
public:
    static MecabDictionaryManager & instance();

    /// Returns the (lazily loaded) Japanese dictionary. Throws on any error.
    MecabDictionaryPtr getJapaneseDictionary();

private:
    MecabDictionaryPtr loadJapaneseDictionary();

    std::mutex mutex;
    MecabDictionaryPtr cached_dictionary TSA_GUARDED_BY(mutex);
};

}

#endif
