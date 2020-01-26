/**
 *  Class describing an AMQP confirm select frame
 * 
 *  @author Marcin Gibula <m.gibula@gmail.com>
 *  @copyright 2017 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class implementation
 */
class ConfirmSelectFrame : public ConfirmFrame
{
private:

    /**
     *  whether to wait for a response
     *  @var    BooleanSet
     */
    BooleanSet _noWait;

protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param   buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        ConfirmFrame::fill(buffer);

        // add boolean
        _noWait.fill(buffer);
    }

public:
    /**
     *  Decode a confirm select frame from a received frame
     *
     *  @param   frame   received frame to decode
     */
    ConfirmSelectFrame(ReceivedFrame& frame) : ConfirmFrame(frame), _noWait(frame) {}

    /**
     *  Construct a confirm select frame
     * 
     *  @param   channel     channel identifier
     *  @return  newly created confirm select frame
     */
    ConfirmSelectFrame(uint16_t channel, bool noWait = false) :
        ConfirmFrame(channel, 1),   //sizeof bool
        _noWait(noWait)
    {}

    /**
     *  Destructor
     */
    virtual ~ConfirmSelectFrame() {}

    /**
     * return the method id
     * @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 10;
    }

    /**
     *  Return whether to wait for a response
     *  @return  boolean
     */
    bool noWait() const
    {
        return _noWait.get(0);
    }
};

/**
 *  end namespace
 */
}

