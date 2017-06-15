/* Copyright (C) 2000 MySQL AB & MySQL Finland AB & TCX DataKonsult AB
                 2012-2016 SkySQL AB, MariaDB Corporation AB
   
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not, write to the Free
   Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02111-1301, USA */

/* Error messages for mysql clients */
/* error messages for the demon is in share/language/errmsg.sys */
#ifndef _errmsg_h_
#define _errmsg_h_

#ifdef	__cplusplus
extern "C" {
#endif
void	init_client_errs(void);
extern const char *client_errors[];	/* Error messages */
extern const char *mariadb_client_errors[];	/* Error messages */
#ifdef	__cplusplus
}
#endif



#define CR_MIN_ERROR		2000	/* For easier client code */
#define CR_MAX_ERROR		2999
#define CER_MIN_ERROR           5000
#define CER_MAX_ERROR           5999
#define CER(X) mariadb_client_errors[(X)-CER_MIN_ERROR]
#define ER(X) client_errors[(X)-CR_MIN_ERROR]
#define CLIENT_ERRMAP		2	/* Errormap used by ma_error() */

#define CR_UNKNOWN_ERROR	2000
#define CR_SOCKET_CREATE_ERROR	2001
#define CR_CONNECTION_ERROR	2002
#define CR_CONN_HOST_ERROR	2003 /* never sent to a client, message only */
#define CR_IPSOCK_ERROR		2004
#define CR_UNKNOWN_HOST		2005
#define CR_SERVER_GONE_ERROR	2006 /* disappeared _between_ queries */
#define CR_VERSION_ERROR	2007
#define CR_OUT_OF_MEMORY	2008
#define CR_WRONG_HOST_INFO	2009
#define CR_LOCALHOST_CONNECTION 2010
#define CR_TCP_CONNECTION	2011
#define CR_SERVER_HANDSHAKE_ERR 2012
#define CR_SERVER_LOST		2013 /* disappeared _during_ a query */
#define CR_COMMANDS_OUT_OF_SYNC 2014
#define CR_NAMEDPIPE_CONNECTION 2015
#define CR_NAMEDPIPEWAIT_ERROR 2016
#define CR_NAMEDPIPEOPEN_ERROR 2017
#define CR_NAMEDPIPESETSTATE_ERROR 2018
#define CR_CANT_READ_CHARSET	2019
#define CR_NET_PACKET_TOO_LARGE 2020
#define CR_SSL_CONNECTION_ERROR 2026
#define CR_MALFORMED_PACKET     2027
#define CR_NO_PREPARE_STMT      2030
#define CR_PARAMS_NOT_BOUND     2031
#define CR_INVALID_PARAMETER_NO  2034
#define CR_INVALID_BUFFER_USE    2035
#define CR_UNSUPPORTED_PARAM_TYPE 2036

#define CR_SHARED_MEMORY_CONNECTION 2037
#define CR_SHARED_MEMORY_CONNECT_ERROR 2038

#define CR_CONN_UNKNOWN_PROTOCOL 2047
#define CR_SECURE_AUTH          2049
#define CR_NO_DATA              2051
#define CR_NO_STMT_METADATA     2052
#define CR_NOT_IMPLEMENTED      2054
#define CR_SERVER_LOST_EXTENDED 2055 /* never sent to a client, message only */
#define CR_STMT_CLOSED          2056
#define CR_NEW_STMT_METADATA    2057
#define CR_ALREADY_CONNECTED    2058
#define CR_AUTH_PLUGIN_CANNOT_LOAD 2059
#define CR_DUPLICATE_CONNECTION_ATTR 2060
#define CR_AUTH_PLUGIN_ERR 2061

/* 
 * MariaDB Connector/C errors: 
 */
#define CR_EVENT_CREATE_FAILED 5000
#define CR_BIND_ADDR_FAILED    5001
#define CR_ASYNC_NOT_SUPPORTED 5002
#define CR_FUNCTION_NOT_SUPPORTED 5003
#define CR_FILE_NOT_FOUND 5004
#define CR_FILE_READ 5005
#define CR_BULK_WITHOUT_PARAMETERS 5006

#endif
