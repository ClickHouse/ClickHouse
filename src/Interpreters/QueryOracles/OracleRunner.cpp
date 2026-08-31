#include <Interpreters/QueryOracles/OracleRunner.h>
#include <Interpreters/QueryOracles/OracleFixture.h>

#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/FieldVisitorToString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace ProfileEvents
{
extern const Event ASTFuzzerOracleMismatches;
}

namespace DB
{

namespace ErrorCodes
{
extern const int AST_FUZZER_ORACLE_MISMATCH;
}

namespace
{

/// The active non-default settings, for reproduction. Moved here verbatim from QueryOracleChecker so
/// the annotation lives in the one mismatch path.
String formatChangedSettings(const ContextPtr & context)
{
    WriteBufferFromOwnString buf;
    bool first = true;
    for (const auto & change : context->getSettingsRef().changes())
    {
        if (!first)
            buf << ", ";
        first = false;
        buf << change.name << "=" << applyVisitor(FieldVisitorToString(), change.value);
    }
    return buf.str();
}

}

void raiseOracleMismatch(const std::string & message, const ContextPtr & context, OracleFixture * fixture)
{
    if (fixture)
        fixture->preserve();

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);

    Exception e(ErrorCodes::AST_FUZZER_ORACLE_MISMATCH, "{}", message);
    const String changed = formatChangedSettings(context);
    if (!changed.empty())
        e.addMessage("Active non-default settings (for reproduction): {}", changed);
    throw e; /// NOLINT(cert-err60-cpp)
}

}
