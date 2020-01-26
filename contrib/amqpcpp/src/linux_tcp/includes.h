/**
 *  Includes.h
 *
 *  The includes that are necessary to compile the optional TCP part of the AMQP library
 *  This file also holds includes that may not be necessary for including the library
 *
 *  @documentation private
 */
// include files from main library
#include "../includes.h"

// c and c++ dependencies
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <netinet/tcp.h>

// utility classes
#include "amqpcpp/linux_tcp/tcpdefines.h"

// mid level includes
#include "amqpcpp/linux_tcp/tcpparent.h"
#include "amqpcpp/linux_tcp/tcphandler.h"
#include "amqpcpp/linux_tcp/tcpconnection.h"

// classes that are very commonly used
#include "addressinfo.h"
