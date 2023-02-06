//
// ApacheConnector.cpp
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ApacheConnector.h"
#include "ApacheApplication.h"
#include "ApacheServerRequest.h"
#include "ApacheServerResponse.h"
#include "ApacheRequestHandlerFactory.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include <memory>
#include "httpd.h"
#include "http_connection.h"
#include "http_config.h"
#include "http_core.h"
#include "http_protocol.h"
#include "http_log.h"
#include "apr.h"
#include "apr_lib.h"
#include "apr_strings.h"
#include "apr_buckets.h"
#include "apr_file_info.h"
#include "apr_hash.h"
#define APR_WANT_STRFUNC
#include "apr_want.h"
#include "http_request.h"
#include "util_filter.h"


using Poco::Net::HTTPServerRequest;
using Poco::Net::HTTPServerResponse;
using Poco::Net::HTTPRequestHandler;
using Poco::Net::HTTPResponse;


extern "C" module AP_MODULE_DECLARE_DATA poco_module;


ApacheRequestRec::ApacheRequestRec(request_rec* pRec):
	_pRec(pRec)
{
}


bool ApacheRequestRec::haveRequestBody()
{
	return ap_should_client_block(_pRec) != 0;
}


int ApacheRequestRec::readRequest(char* buffer, int length)
{
	return ap_get_client_block(_pRec, buffer, length);
}


void ApacheRequestRec::writeResponse(const char* buffer, int length)
{
	ap_rwrite(buffer, length, _pRec);
}


void ApacheRequestRec::redirect(const std::string& uri, int status)
{
	apr_table_set(_pRec->headers_out, "Location", uri.c_str());
	_pRec->connection->keepalive = AP_CONN_CLOSE;
	_pRec->status = status;
	ap_set_keepalive(_pRec);
	ap_send_error_response(_pRec, 0);
}


void ApacheRequestRec::sendErrorResponse(int status)
{
	_pRec->connection->keepalive = AP_CONN_CLOSE;
	_pRec->status = status;
	ap_set_keepalive(_pRec);

	ap_send_error_response(_pRec, 0);
}


void ApacheRequestRec::addHeader(const std::string& key, const std::string& value)
{
	const apr_array_header_t *arr = apr_table_elts(_pRec->headers_out);
	apr_table_add(const_cast<apr_table_t*>(reinterpret_cast<const apr_table_t*>(arr)), key.c_str(), value.c_str());
}


void ApacheRequestRec::setContentType(const std::string& mediaType)
{
	ap_set_content_type(_pRec, mediaType.c_str());
}


int ApacheRequestRec::sendFile(const std::string& path, unsigned int fileSize, const std::string& mediaType)
{
	apr_file_t *thefile = 0;
	apr_finfo_t finfo;
	apr_size_t nBytes;

	// setting content-type
	ap_set_content_type(_pRec, mediaType.c_str());

	// opening file
	if (apr_file_open(&thefile, path.c_str(), APR_READ, APR_UREAD | APR_GREAD, _pRec->pool) == APR_SUCCESS)
	{
		// getting fileinfo
		apr_file_info_get(&finfo, APR_FINFO_NORM, thefile);

		// setting last-updated & co
		ap_update_mtime(_pRec, finfo.mtime);
		ap_set_last_modified(_pRec);
		ap_set_content_length(_pRec, fileSize);

		// sending file
		ap_send_fd(thefile, _pRec, 0, fileSize, &nBytes);

		// well done
		return 0;
	}

	// file not opened successfully -> produce an exception in C++ code
	return 1;
}


void ApacheRequestRec::copyHeaders(ApacheServerRequest& request)
{
	const apr_array_header_t* arr = apr_table_elts(_pRec->headers_in);
	const apr_table_entry_t* elts = (const apr_table_entry_t *)arr->elts;

	request.setMethod(_pRec->method);
	request.setURI(_pRec->unparsed_uri);

	// iterating over raw-headers and printing them
	for (int i = 0; i < arr->nelts; i++)
	{
		request.add(elts[i].key, elts[i].val);
	}
}


void ApacheConnector::log(const char* file, int line, int level, int status, const char *text)
{
	ap_log_error(file, line, level, 0, NULL, "%s", text);
}


extern "C" int ApacheConnector_handler(request_rec *r)
{
	ApacheRequestRec rec(r);
	ApacheApplication& app(ApacheApplication::instance());
	
	try
	{
		// ensure application is ready
		app.setup();
		
		// if the ApacheRequestHandler declines handling - we stop
		// request handling here and let other modules do their job!
		if (!app.factory().mustHandle(r->uri))
			return DECLINED;

	    apr_status_t rv;
		if ((rv = ap_setup_client_block(r, REQUEST_CHUNKED_DECHUNK))) 
			return rv;

#ifndef POCO_ENABLE_CPP11
		std::auto_ptr<ApacheServerRequest> pRequest(new ApacheServerRequest(
			&rec, 
			r->connection->local_ip, 
			r->connection->local_addr->port,
			r->connection->remote_ip, 
			r->connection->remote_addr->port));

		std::auto_ptr<ApacheServerResponse> pResponse(new ApacheServerResponse(pRequest.get()));
#else
		std::unique_ptr<ApacheServerRequest> pRequest(new ApacheServerRequest(
			&rec,
			r->connection->local_ip,
			r->connection->local_addr->port,
			r->connection->remote_ip,
			r->connection->remote_addr->port));

		std::unique_ptr<ApacheServerResponse> pResponse(new ApacheServerResponse(pRequest.get()));
#endif // POCO_ENABLE_CPP11

		// add header information to request
		rec.copyHeaders(*pRequest);
		
		try
		{

#ifndef POCO_ENABLE_CPP11
			std::auto_ptr<HTTPRequestHandler> pHandler(app.factory().createRequestHandler(*pRequest));
#else
			std::unique_ptr<HTTPRequestHandler> pHandler(app.factory().createRequestHandler(*pRequest));
#endif // POCO_ENABLE_CPP11

			if (pHandler.get())
			{				
				pHandler->handleRequest(*pRequest, *pResponse);
			}
			else
			{
				pResponse->sendErrorResponse(HTTP_NOT_IMPLEMENTED);
			}
		}
		catch (...)
		{
			pResponse->sendErrorResponse(HTTP_INTERNAL_SERVER_ERROR);
			throw;
		}
	}
	catch (Poco::Exception& exc)
	{
		ApacheConnector::log(__FILE__, __LINE__, ApacheConnector::PRIO_ERROR, 0, exc.displayText().c_str());
	}
	catch (...)
	{
		ApacheConnector::log(__FILE__, __LINE__, ApacheConnector::PRIO_ERROR, 0, "Unknown exception");
	}
	return OK;
}


extern "C" void ApacheConnector_register_hooks(apr_pool_t *p)
{
	ap_hook_handler(ApacheConnector_handler, NULL, NULL, APR_HOOK_MIDDLE);
}


extern "C" const char* ApacheConnector_uris(cmd_parms *cmd, void *in_dconf, const char *in_str)
{
	try
	{
		ApacheApplication::instance().factory().handleURIs(in_str);
	}
	catch (Poco::Exception& exc)
	{
		ApacheConnector::log(__FILE__, __LINE__, ApacheConnector::PRIO_ERROR, 0, exc.displayText().c_str());
	}
	catch (...)
	{
		ApacheConnector::log(__FILE__, __LINE__, ApacheConnector::PRIO_ERROR, 0, "Unknown exception");
	}
    return 0;
}


extern "C" const char* ApacheConnector_config(cmd_parms *cmd, void *in_dconf, const char *in_str)
{
	try
	{
		ApacheApplication::instance().loadConfiguration(in_str);
	}
	catch (Poco::Exception& exc)
	{
		ApacheConnector::log(__FILE__, __LINE__, ApacheConnector::PRIO_ERROR, 0, exc.displayText().c_str());
	}
	catch (...)
	{
		ApacheConnector::log(__FILE__, __LINE__, ApacheConnector::PRIO_ERROR, 0, "Unknown exception");
	}
    return 0;
}


extern "C" const command_rec ApacheConnector_cmds[] = 
{
    AP_INIT_RAW_ARGS(
		"AddPocoRequestHandler", 
		reinterpret_cast<cmd_func>(ApacheConnector_uris), 
		NULL,
		RSRC_CONF, 
		"POCO RequestHandlerFactory class name followed by shared library path followed by a list of ' ' separated URIs that must be handled by this module."),
    AP_INIT_RAW_ARGS(
		"AddPocoConfig", 
		reinterpret_cast<cmd_func>(ApacheConnector_config), 
		NULL,
		RSRC_CONF, 
		"Path of the POCO configuration file."),
    { NULL }
};


module AP_MODULE_DECLARE_DATA poco_module = 
{
	STANDARD20_MODULE_STUFF,
	NULL,
	NULL,
	NULL,
	NULL,
	ApacheConnector_cmds,
	ApacheConnector_register_hooks
};
