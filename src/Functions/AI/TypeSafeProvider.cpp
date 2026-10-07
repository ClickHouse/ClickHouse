#include <Functions/AI/TypeSafeProvider.h>
#include <Common/Exception.h>

#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>

#include <algorithm>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int MALFORMED_AI_PROVIDER_RESPONSE;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

const char * questionTypeName(AIDecisionQuestionType type)
{
    switch (type)
    {
        case AIDecisionQuestionType::Choice:
            return "choice";
    }
    std::unreachable();
}

}

TypeSafeProvider::TypeSafeProvider(const String & endpoint_, const String & api_key_)
    : api_key(api_key_)
    , uri(endpoint_)
{
}

void TypeSafeProvider::call(const AIRequest & /*ai_request*/, const ConnectionTimeouts & /*timeouts*/, AIResponse & /*response*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "AI provider 'typesafe' does not support chat requests");
}

void TypeSafeProvider::decide(const AIDecisionRequest & ai_decision_request, const ConnectionTimeouts & timeouts, AIDecisionResponse & response)
{
    response = {};

    Poco::JSON::Object::Ptr questions = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    for (const auto & question : ai_decision_request.questions)
    {
        Poco::JSON::Object::Ptr criteria = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
        for (const auto & [label, description] : question.criteria)
            criteria->set(label, description);

        Poco::JSON::Object::Ptr question_obj = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
        question_obj->set("type", questionTypeName(question.type));
        question_obj->set("instructions", question.instructions);
        question_obj->set("criteria", criteria);
        questions->set(question.key, question_obj);
    }

    Poco::JSON::Object::Ptr root = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    root->set("model", ai_decision_request.model);
    root->set("state", ai_decision_request.state);
    root->set("questions", questions);

    std::ostringstream body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    root->stringify(body_stream);
    String body = std::move(body_stream).str();

    String response_body = postJSONToAIProvider(uri, api_key, ai_decision_request.function_name, body, timeouts);

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    const auto & json_obj = json_result.extract<Poco::JSON::Object::Ptr>();
    if (!json_obj)
        throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE, "AI decision response is not a JSON object");

    /// A malformed body was still charged for, so read the usage before the checks below can throw.
    if (auto usage = json_obj->getObject("usage"))
    {
        response.input_tokens = usage->optValue<UInt64>("input_tokens", 0);
        response.output_tokens = usage->optValue<UInt64>("output_tokens", 0);
    }

    auto answers = json_obj->getObject("answers");
    if (!answers)
        throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE, "AI decision response is missing 'answers' object");

    for (const auto & question : ai_decision_request.questions)
    {
        auto answer_obj = answers->getObject(question.key);
        if (!answer_obj)
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI decision response is missing the answer for question '{}'", question.key);

        const String expected_type = questionTypeName(question.type);
        const String type = answer_obj->optValue<String>("type", "");
        if (type != expected_type)
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI decision response answer for question '{}' has type '{}', expected '{}'",
                question.key, sanitizeForLog(type), expected_type);

        AIDecisionAnswer answer;
        answer.choice = answer_obj->optValue<String>("choice", "");
        bool known_label = std::any_of(
            question.criteria.begin(), question.criteria.end(), [&](const auto & criterion) { return criterion.first == answer.choice; });
        if (!known_label)
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI decision response answer for question '{}' has choice '{}', which is not one of the labels",
                question.key, sanitizeForLog(answer.choice));

        const auto confidence = answer_obj->get("confidence");
        if (!confidence.isNumeric())
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI decision response answer for question '{}' is missing numeric 'confidence'", question.key);
        answer.confidence = confidence.convert<Float64>();
        if (!(answer.confidence >= 0 && answer.confidence <= 1))
            throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
                "AI decision response answer for question '{}' has 'confidence' {} outside [0, 1]", question.key, answer.confidence);

        if (auto probabilities = answer_obj->getObject("probabilities"))
        {
            for (const auto & [label, probability] : *probabilities)
                if (probability.isNumeric())
                    answer.probabilities.emplace(label, probability.convert<Float64>());
        }

        response.answers.emplace(question.key, std::move(answer));
    }
}

}
