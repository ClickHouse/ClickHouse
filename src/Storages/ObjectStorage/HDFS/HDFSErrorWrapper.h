#pragma once

#include "config.h"

#if USE_HDFS
#include <Storages/ObjectStorage/HDFS/HDFSCommon.h>
#include <base/types.h>

namespace DB
{

class HDFSErrorWrapper
{
public:
    mutable HDFSBuilderWrapper builder;

    HDFSErrorWrapper(
            const std::string & hdfs_uri_,
            const Poco::Util::AbstractConfiguration & config_
    );

    template <typename R, typename F, typename... P> R wrapErr(F fn, const P... p) const
    {
        R r = fn(p...);
        #if USE_KRB5
        if (errno == EACCES) // NOLINT
        {
            builder.runKinit(); // krb5 keytab reinitialization
            r = fn(p...);
        }
        #endif // USE_KRB5
        return r;
    }

};

}

#endif
