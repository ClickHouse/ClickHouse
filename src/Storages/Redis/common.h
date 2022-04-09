#include <Poco/JSON/Object.h>
#include <base/types.h>
#include <boost/algorithm/string.hpp>

struct Message {
    String stream;
    String key;
    String seq_num;
    String timestamp;
    String attrs;
};

using Attrs = std::vector<std::pair<std::string, std::string>>;
using Item = std::pair<std::string, sw::redis::Optional<Attrs>>;
using ItemStream = std::vector<Item>;
using StreamsOutput = std::vector<std::pair<std::string, ItemStream>>;
using Messages = std::vector<Message>;

Messages ConvertStreamsOutputToMessages(const StreamsOutput& output) {
    messages.clear();
    for (const auto& [stream_name, msg_stream] : output) {
        for (const auto& [id, attrs] : msg_stream) {
            Message msg;
            streams_with_ids[stream_name] = id;
            msg.stream = stream_name;
            msg.key = id;
            std::vector<std::string> tmp;
            boost::split(tmp, id, boost::is_any_of("-"));
            msg.timestamp = tmp.front();
            msg.seq_num = tmp.back();

            Poco::JSON::Object json;
            if (attrs.has_value()) {
                for (const auto& [key, value] : attrs.value()) {
                    json.set(key, value);
                }
            }
            std::stringstream stream;
            json.stringify(stream);
            msg.attrs = stream.str();
            messages.push_back(std::move(msg));
        }
    }
    current = messages.begin();
}
