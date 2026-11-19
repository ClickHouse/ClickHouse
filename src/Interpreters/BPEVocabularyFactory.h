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
  * `vocabulary` holds the same content inline instead. A loaded vocabulary is shared by the queries
  * that name it, and is reloaded when its file or its inline content changes.
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
