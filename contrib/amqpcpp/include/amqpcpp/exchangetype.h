/**
 *  ExchangeType.h
 *
 *  The various exchange types that are supported
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  The class
 */
enum ExchangeType
{
    fanout,
    direct,
    topic,
    headers,
    consistent_hash
};

/**
 *  End of namespace
 */
}

