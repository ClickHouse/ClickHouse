/**
 *  AMQP.h
 *
 *  Starting point for all includes of the Copernica AMQP library
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2015 - 2018 Copernica BV
 */

#pragma once

// base C++ include files
#include <vector>
#include <string>
#include <memory>
#include <map>
#include <unordered_map>
#include <queue>
#include <limits>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <utility>
#include <iostream>
#include <algorithm>
#include <functional>

// base C include files
#include <stdint.h>
#include <math.h>

// fix strcasecmp on non linux platforms
#if (defined(_WIN16) || defined(_WIN32) || defined(_WIN64) || defined(__WINDOWS__)) && !defined(__CYGWIN__)
    #define strcasecmp _stricmp 
#endif

// forward declarations
#include "amqpcpp/classes.h"

// utility classes
#include "amqpcpp/endian.h"
#include "amqpcpp/buffer.h"
#include "amqpcpp/bytebuffer.h"
#include "amqpcpp/receivedframe.h"
#include "amqpcpp/outbuffer.h"
#include "amqpcpp/watchable.h"
#include "amqpcpp/monitor.h"

// amqp types
#include "amqpcpp/field.h"
#include "amqpcpp/numericfield.h"
#include "amqpcpp/decimalfield.h"
#include "amqpcpp/stringfield.h"
#include "amqpcpp/booleanset.h"
#include "amqpcpp/fieldproxy.h"
#include "amqpcpp/table.h"
#include "amqpcpp/array.h"

// envelope for publishing and consuming
#include "amqpcpp/metadata.h"
#include "amqpcpp/envelope.h"
#include "amqpcpp/message.h"

// mid level includes
#include "amqpcpp/exchangetype.h"
#include "amqpcpp/flags.h"
#include "amqpcpp/callbacks.h"
#include "amqpcpp/deferred.h"
#include "amqpcpp/deferredconsumer.h"
#include "amqpcpp/deferredqueue.h"
#include "amqpcpp/deferreddelete.h"
#include "amqpcpp/deferredcancel.h"
#include "amqpcpp/deferredconfirm.h"
#include "amqpcpp/deferredget.h"
#include "amqpcpp/deferredpublisher.h"
#include "amqpcpp/channelimpl.h"
#include "amqpcpp/channel.h"
#include "amqpcpp/login.h"
#include "amqpcpp/address.h"
#include "amqpcpp/connectionhandler.h"
#include "amqpcpp/connectionimpl.h"
#include "amqpcpp/connection.h"
#include "amqpcpp/openssl.h"
