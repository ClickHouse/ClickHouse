#pragma once

#include <IO/ReadWriteBufferFromHTTP.h>
#include <Common/RemoteHostFilter.h>

namespace DB
{

namespace detail
{
    template <typename UpdatableSessionPtr>
    class ReadWriteBufferFromHTTPWithHostFilterBase : public ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>
    {
    protected:
        RemoteHostFilter remote_host_filter;
    public:
        using HTTPHeaderEntry = std::tuple<std::string, std::string>;
        using HTTPHeaderEntries = std::vector<HTTPHeaderEntry>;
        using OutStreamCallback = std::function<void(std::ostream &)>;
        using parent = ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>;

        explicit ReadWriteBufferFromHTTPWithHostFilterBase(UpdatableSessionPtr session_,
             Poco::URI uri_,
             const std::string & method_ = {},
             OutStreamCallback out_stream_callback_ = {},
             const Poco::Net::HTTPBasicCredentials & credentials_ = {},
             size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
             HTTPHeaderEntries http_header_entries_ = {},
             const RemoteHostFilter & remote_host_filter_ = {})
                : parent(session_, uri_, method_, out_stream_callback_, credentials_, buffer_size_, http_header_entries_)
                , remote_host_filter {remote_host_filter_}
        {
            Poco::Net::HTTPResponse response;

            parent::istr = parent::call(parent::uri, response);

            while (isRedirect(response.getStatus()))
            {
                Poco::URI uri_redirect(response.get("Location"));
                remote_host_filter.checkURL(uri_redirect);

                parent::session->updateSession(uri_redirect);

                parent::istr = parent::call(uri_redirect,response);
            }

            try
            {
                parent::impl = std::make_unique<ReadBufferFromIStream>(*parent::istr, buffer_size_);
            }
            catch (const Poco::Exception & e)
            {
                auto sess = parent::session->getSession();
                sess->attachSessionData(e.message());
                throw;
            }
        }
    };
}


class ReadWriteBufferFromHTTPWithHostFilter : public detail::ReadWriteBufferFromHTTPWithHostFilterBase<std::shared_ptr<UpdatableSession>>
{
    using Parent = detail::ReadWriteBufferFromHTTPWithHostFilterBase<std::shared_ptr<UpdatableSession>>;

public:
    explicit ReadWriteBufferFromHTTPWithHostFilter(Poco::URI uri_,
         const std::string & method_ = {},
         OutStreamCallback out_stream_callback_ = {},
         const ConnectionTimeouts & timeouts = {},
         const DB::SettingUInt64 max_redirects = 0,
         const RemoteHostFilter & remote_host_filter_ = {},
         const Poco::Net::HTTPBasicCredentials & credentials_ = {},
         size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
         const HTTPHeaderEntries & http_header_entries_ = {})
         : Parent(std::make_shared<UpdatableSession>(uri_, timeouts, max_redirects), uri_, method_, out_stream_callback_, credentials_, buffer_size_, http_header_entries_, remote_host_filter_)
    {
    }
};
}
