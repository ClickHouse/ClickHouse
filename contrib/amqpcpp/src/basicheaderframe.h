/**
 *  Class describing an AMQP basic header frame
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
#include "headerframe.h"
#include "amqpcpp/metadata.h"
#include "amqpcpp/envelope.h"
#include "amqpcpp/connectionimpl.h"
#include "amqpcpp/deferredreceiver.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class implementation
 */
class BasicHeaderFrame : public HeaderFrame
{
private:
     /**
     *  Weight field, unused but must be sent, always value 0;
     *  @var uint16_t
     */
    uint16_t _weight = 0;

    /**
     *  Body size, sum of the sizes of all body frames following the content header
     *  @var uint64_t
     */
    uint64_t _bodySize;

    /**
     *  The meta data
     *  @var MetaData
     */
    MetaData _metadata;

protected:
    /**
     *  Encode a header frame to a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer &buffer) const override
    {
        // call base
        HeaderFrame::fill(buffer);

        // fill own fields.
        buffer.add(_weight);
        buffer.add(_bodySize);

        // the meta data
        _metadata.fill(buffer);
    }

public:
    /**
     *  Construct an empty basic header frame
     *
     *  All options are set using setter functions.
     *
     *  @param  channel     channel we're working on
     *  @param  envelope    the envelope
     */
    BasicHeaderFrame(uint16_t channel, const Envelope &envelope) :
        HeaderFrame(channel, 10 + envelope.size()), // there are at least 10 bytes sent, weight (2), bodySize (8), plus the size of the meta data
        _bodySize(envelope.bodySize()),
        _metadata(envelope)
    {}

    /**
     *  Constructor to parse incoming frame
     *  @param  frame
     */
    BasicHeaderFrame(ReceivedFrame &frame) :
        HeaderFrame(frame),
        _weight(frame.nextUint16()),
        _bodySize(frame.nextUint64()),
        _metadata(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~BasicHeaderFrame() = default;

    /**
     *  Size of the body
     *  @return uint64_t
     */
    uint64_t bodySize() const
    {
        return _bodySize;
    }

    /**
     *  The metadata sent in this frame
     *
     *  @return All the metadata for this message
     */
    const MetaData &metaData() const
    {
        return _metadata;
    }

    /**
     *  The class ID
     *  @return uint16_t
     */
    virtual uint16_t classID() const override
    {
        return 60;
    }

    /**
     *  Process the frame
     *  @param  connection      The connection over which it was received
     *  @return bool            Was it succesfully processed?
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // we need the appropriate channel
        auto channel = connection->channel(this->channel());
        
        // we need a channel
        if (channel == nullptr) return false;
        
        // do we have an object that is receiving this data?
        auto *receiver = channel->receiver();

        // check if we have a valid channel and consumer
        if (receiver == nullptr) return false;

        // the channel can process the frame
        receiver->process(*this);

        // done
        return true;
    }
};

/**
 *  End namespace
 */
}

