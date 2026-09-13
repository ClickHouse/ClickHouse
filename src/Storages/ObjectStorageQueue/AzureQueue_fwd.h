#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/re2.h>
#include <optional>

namespace AzureQueue
{

static constexpr auto TABLE_ENGINE_NAME = "AzureQueue";
static constexpr auto DEFAULT_MASKING_RULE = [](const DB::Field &){ return "'[HIDDEN]'"; };

using ValueMaskingFunc = std::function<std::optional<std::string>(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"after_processing_move_connection_string", [](const DB::Field & value) -> std::optional<std::string>
    {
        /// Not a String means there is nothing to mask; see the `nats_url` rule for why a rule
        /// must not throw.
        std::string masked_value;
        if (!value.tryGet<std::string>(masked_value))
            return {};
        static re2::RE2 account_key_pattern = "AccountKey=.*?(;|$)";
        RE2::Replace(&masked_value, account_key_pattern, "AccountKey=[HIDDEN]\\1");
        static re2::RE2 sas_signature_pattern = "SharedAccessSignature=.*?(;|$)";
        RE2::Replace(&masked_value, sas_signature_pattern, "SharedAccessSignature=[HIDDEN]\\1");
        return fmt::format("'{}'", masked_value);
    }},
};

}
