/**
 *  Includes.h
 *
 *  The includes that are necessary to compile the AMQP library
 *  This file also holds includes that may not be necessary for including the library
 *
 *  @documentation private
 */

// c and c++ dependencies
#include <stdlib.h>
#include <string.h> // TODO cstring
#include <stdint.h>
#include <string>
#include <memory>
#include <limits>
#include <ostream>
#include <math.h>
#include <map>
#include <algorithm>
#include <unordered_map>
#include <vector>
#include <queue>

#include <sys/types.h> // TODO is this needed

#include <functional>
#include <stdexcept>

// TODO make this nice
#ifdef _MSC_VER 
//not #if defined(_WIN32) || defined(_WIN64) because we have strncasecmp in mingw
#define strncasecmp _strnicmp
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
#include "amqpcpp/copiedbuffer.h"
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
#include "amqpcpp/deferredpublisher.h"
#include "amqpcpp/deferredqueue.h"
#include "amqpcpp/deferreddelete.h"
#include "amqpcpp/deferredcancel.h"
#include "amqpcpp/deferredconfirm.h"
#include "amqpcpp/deferredget.h"
#include "amqpcpp/channelimpl.h"
#include "amqpcpp/channel.h"
#include "amqpcpp/login.h"
#include "amqpcpp/address.h"
#include "amqpcpp/connectionhandler.h"
#include "amqpcpp/connectionimpl.h"
#include "amqpcpp/connection.h"

// classes that are very commonly used
#include "amqpcpp/exception.h"
#include "amqpcpp/protocolexception.h"
#include "amqpcpp/frame.h"
#include "extframe.h"
#include "methodframe.h"
#include "headerframe.h"
#include "connectionframe.h"
#include "channelframe.h"
#include "exchangeframe.h"
#include "queueframe.h"
#include "basicframe.h"
#include "confirmframe.h"
#include "transactionframe.h"


