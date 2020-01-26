/**
 *  Callbacks.h
 *
 *  Class storing deferred callbacks of different type.
 *
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <string>
#include <functional>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Forward declarations
 */
class Message;
class MetaData;

/**
 *  Generic callbacks that are used by many deferred objects
 */
using SuccessCallback       =   std::function<void()>;
using ErrorCallback         =   std::function<void(const char *message)>;
using FinalizeCallback      =   std::function<void()>;

/**
 *  Declaring and deleting a queue
 */
using QueueCallback         =   std::function<void(const std::string &name, uint32_t messagecount, uint32_t consumercount)>;
using DeleteCallback        =   std::function<void(uint32_t deletedmessages)>;

/**
 *  When retrieving the size of a queue in some way
 */
using EmptyCallback         =   std::function<void()>;
using CountCallback         =   std::function<void(uint32_t messagecount)>;
using SizeCallback          =   std::function<void(uint64_t messagesize)>;

/**
 *  Starting and stopping a consumer
 */
using ConsumeCallback       =   std::function<void(const std::string &consumer)>;
using CancelCallback        =   std::function<void(const std::string &consumer)>;

/**
 *  Receiving messages, either via consume(), get() or as returned messages
 *  The following methods receive the returned message in multiple parts
 */
using StartCallback         =   std::function<void(const std::string &exchange, const std::string &routingkey)>;
using HeaderCallback        =   std::function<void(const MetaData &metaData)>;
using DataCallback          =   std::function<void(const char *data, size_t size)>;
using DeliveredCallback     =   std::function<void(uint64_t deliveryTag, bool redelivered)>;

/**
 *  For returned messages amqp-cpp first calls a return-callback before the start,
 *  header and data callbacks are called. Instead of the deliver-callback, a
 *  returned-callback is called.
 */
using ReturnCallback        =   std::function<void(int16_t code, const std::string &message)>;
using ReturnedCallback      =   std::function<void()>;

/**
 *  If you do not want to merge all data into a single string, you can als
 *  implement callbacks that return the collected message.
 */
using MessageCallback       =   std::function<void(const Message &message, uint64_t deliveryTag, bool redelivered)>;
using BounceCallback        =   std::function<void(const Message &message, int16_t code, const std::string &description)>;

/**
 * When using publisher confirms, AckCallback is called when server confirms that message is received
 * and processed. NackCallback is called otherwise.
 */
using AckCallback           =   std::function<void(uint64_t deliveryTag, bool multiple)>;
using NackCallback          =   std::function<void(uint64_t deliveryTag, bool multiple, bool requeue)>;

/**
 *  End namespace
 */
}
