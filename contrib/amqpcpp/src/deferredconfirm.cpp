/**
 *  DeferredConfirm.cpp
 *
 *  Implementation file for the DeferredConfirm class
 *
 *  @author Marcin Gibula <m.gibula@gmail.com>
 *  @copyright 2018 Copernica BV
 */
#include "includes.h"
#include "basicackframe.h"
#include "basicnackframe.h"

/**
 *  Namespace
 */
namespace AMQP {

/**
 *  Process an ACK frame
 *
 *  @param  frame   The frame to process
 */
void DeferredConfirm::process(BasicAckFrame &frame)
{
    if (_ackCallback) _ackCallback(frame.deliveryTag(), frame.multiple());
}

/**
 *  Process a NACK frame
 *
 *  @param  frame   The frame to process
 */
void DeferredConfirm::process(BasicNackFrame &frame)
{
    if (_nackCallback) _nackCallback(frame.deliveryTag(), frame.multiple(), frame.requeue());
}

/**
 *  End namespace
 */
}

