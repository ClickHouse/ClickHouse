#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/CastOverloadResolverImpl.h>


namespace DB
{

FunctionOverloadResolverPtr createInternalCastOverloadResolver(CastType type, std::optional<CastDiagnostic> diagnostic)
{
    switch (type)
    {
        case CastType::nonAccurate:
            return CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(diagnostic);
        case CastType::accurate:
            return CastInternalOverloadResolver<CastType::accurate>::createImpl(diagnostic);
        case CastType::accurateOrNull:
            return CastInternalOverloadResolver<CastType::accurateOrNull>::createImpl(diagnostic);
    }
}

REGISTER_FUNCTION(CastOverloadResolvers)
{
    factory.registerFunction<CastInternalOverloadResolver<CastType::nonAccurate>>({}, FunctionFactory::CaseInsensitive);
    /// Note: "internal" (not affected by null preserving setting) versions of accurate cast functions are unneeded.

    factory.registerFunction<CastOverloadResolver<CastType::nonAccurate>>({}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<CastOverloadResolver<CastType::accurate>>();
    factory.registerFunction<CastOverloadResolver<CastType::accurateOrNull>>();
}

}
