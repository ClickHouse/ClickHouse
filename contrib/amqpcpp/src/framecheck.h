/**
 *  FrameCheck.h
 *
 *  Class that checks incoming frames for their size
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace AMQP {
    
/**
 *  Internal helper class that checks if there is enough room left in the frame
 */
class FrameCheck
{
private:
    /**
     *  The frame
     *  @var ReceivedFrame
     */
    ReceivedFrame *_frame;
    
    /**
     *  The size that is checked
     *  @var size_t
     */
    size_t _size;
    
public:
    /**
     *  Constructor
     *  @param  frame
     *  @param  size
     */
    FrameCheck(ReceivedFrame *frame, size_t size) : _frame(frame), _size(size)
    {
        // no problem is there are still enough bytes left
        if (frame->_buffer.size() - frame->_skip >= size) return;
        
        // frame buffer is too small
        throw ProtocolException("frame out of range");
    }
    
    /**
     *  Destructor
     */
    virtual ~FrameCheck()
    {
        // update the number of bytes to skip
        _frame->_skip += (uint32_t)_size;
    }
};

/**
 *  End namespace
 */
}

