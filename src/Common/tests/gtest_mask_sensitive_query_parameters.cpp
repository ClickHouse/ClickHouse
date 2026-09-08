#include <Common/maskSensitiveQueryParameters.h>

#include <gtest/gtest.h>

using DB::maskSensitiveQueryParametersInURI;

TEST(MaskSensitiveQueryParameters, NoQueryString)
{
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/"), "/");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/path/to/resource"), "/path/to/resource");
    EXPECT_EQ(maskSensitiveQueryParametersInURI(""), "");
}

TEST(MaskSensitiveQueryParameters, NonSensitiveParametersUnchanged)
{
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?query=SELECT+1"), "/?query=SELECT+1");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?database=db&query=SELECT+1"), "/?database=db&query=SELECT+1");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?max_threads=4"), "/?max_threads=4");
}

TEST(MaskSensitiveQueryParameters, RedactsParamSecretKey)
{
    EXPECT_EQ(
        maskSensitiveQueryParametersInURI("/?param_secret_key=TOPSECRET&query=SELECT+1"),
        "/?param_secret_key=[HIDDEN]&query=SELECT+1");
}

TEST(MaskSensitiveQueryParameters, RedactsVariousSensitiveNames)
{
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?password=p"), "/?password=[HIDDEN]");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?passwd=p"), "/?passwd=[HIDDEN]");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?access_token=t"), "/?access_token=[HIDDEN]");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?my_credential=c"), "/?my_credential=[HIDDEN]");
    EXPECT_EQ(
        maskSensitiveQueryParametersInURI("/?param_aws_secret_access_key=AKIA"),
        "/?param_aws_secret_access_key=[HIDDEN]");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?x-amz-signature=abc"), "/?x-amz-signature=[HIDDEN]");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?sig=abc"), "/?sig=[HIDDEN]");
}

TEST(MaskSensitiveQueryParameters, CaseInsensitiveName)
{
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?PARAM_SECRET_KEY=x"), "/?PARAM_SECRET_KEY=[HIDDEN]");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?Password=x"), "/?Password=[HIDDEN]");
}

TEST(MaskSensitiveQueryParameters, DoesNotOverRedact)
{
    /// "sig" is only matched exactly, not as a substring.
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?design=modern"), "/?design=modern");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?signal=on"), "/?signal=on");
}

TEST(MaskSensitiveQueryParameters, EncodedSensitiveName)
{
    /// The server percent-decodes the parameter NAME before interpreting it (HTMLForm::readQuery),
    /// so an encoded sensitive name must be classified by its decoded form, not its raw spelling.
    /// The raw (still-encoded) name is preserved in the output; only the value is hidden.
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?pass%77ord=LEAKME&query=SELECT+1"), "/?pass%77ord=[HIDDEN]&query=SELECT+1");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?%73ecret=LEAKME"), "/?%73ecret=[HIDDEN]");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?%73ig=LEAKME"), "/?%73ig=[HIDDEN]");
    /// Uppercase hex digits in the escape are accepted too.
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?pass%77ORD=x"), "/?pass%77ORD=[HIDDEN]");
    /// '+' in a name decodes to a space; "secret key" still matches the "secret" substring.
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?secret+key=x"), "/?secret+key=[HIDDEN]");
    /// A malformed escape is left as-is and must not falsely match or crash.
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?desi%6n=modern"), "/?desi%6n=modern");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?design%=modern"), "/?design%=modern");
}

TEST(MaskSensitiveQueryParameters, MixedParameters)
{
    EXPECT_EQ(
        maskSensitiveQueryParametersInURI("/?database=db&param_secret_key=s&query=SELECT+1&password=p"),
        "/?database=db&param_secret_key=[HIDDEN]&query=SELECT+1&password=[HIDDEN]");
}

TEST(MaskSensitiveQueryParameters, EmptyAndValuelessParameters)
{
    /// Flags without a value are left untouched, and only the matching value is hidden.
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?flag&password=p"), "/?flag&password=[HIDDEN]");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?password="), "/?password=[HIDDEN]");
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?token=&query=x"), "/?token=[HIDDEN]&query=x");
}

TEST(MaskSensitiveQueryParameters, EmptyQueryString)
{
    EXPECT_EQ(maskSensitiveQueryParametersInURI("/?"), "/?");
}
