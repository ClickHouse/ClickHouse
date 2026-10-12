#pragma once

#include <Interpreters/BPETokenizer.h>

#include <base/types.h>

#include <mutex>
#include <unordered_map>

namespace Poco::Util { class AbstractConfiguration; }


namespace DB
{

/** The BPE vocabularies declared in the server configuration, loaded when a query first names one:
  *
  *     <bpe_vocabularies>
  *         <cl100k_base>
  *             <path>/var/lib/clickhouse/tokenizers/cl100k_base.tiktoken</path>
  *             <pretokenizer>cl100k</pretokenizer>
  *         </cl100k_base>
  *     </bpe_vocabularies>
  *
  * `vocabulary` holds the same content inline instead, for a vocabulary small enough to be written
  * out in the configuration.
  *
  * A vocabulary is immutable and is shared by every query that names it. It is reloaded when the
  * file it came from changes, which is what the modification time and size it was read at are for:
  * a vocabulary of a hundred thousand tokens is not something to parse per query, and not something
  * to pin in memory for the lifetime of the server either if the file behind it has moved on.
  */
class BPEVocabularyFactory
{
public:
    static BPEVocabularyFactory & instance();

    BPEVocabularyPtr get(const String & name, const Poco::Util::AbstractConfiguration & config);

private:
    struct Entry
    {
        /// Empty for a vocabulary written out in the configuration.
        String path;
        BPEPretokenizer pretokenizer = BPEPretokenizer::Cl100k;
        /// Of the file, or of the inline content, so that a changed vocabulary is picked up.
        UInt64 modification_time = 0;
        UInt64 size = 0;
        BPEVocabularyPtr vocabulary;
    };

    std::mutex mutex;
    std::unordered_map<String, Entry> loaded TSA_GUARDED_BY(mutex);
};

}
