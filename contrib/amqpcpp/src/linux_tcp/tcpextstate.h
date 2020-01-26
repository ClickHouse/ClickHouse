/**
 *  TcpExtState.h
 *
 *  Extended state that also contains the socket filedescriptor
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Begin of namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class TcpExtState : public TcpState
{
protected:
    /**
     *  The filedescriptor
     *  @var int
     */
    int _socket;
    
    /**
     *  Clean-up the socket, and call the onClosed() method
     */
    void cleanup()
    {
        // do nothing if no longer connected
        if (_socket < 0) return;
        
        // tell handler that the socket is idle and we're no longer interested in events
        _parent->onIdle(this, _socket, 0);
        
        // close the socket
        ::close(_socket);
        
        // forget the socket
        _socket = -1;
        
        // tell the handler that the connection is now lost
        _parent->onLost(this);
    }
    
    
protected:
    /**
     *  Constructor
     *  @param  parent
     */
    TcpExtState(TcpParent *parent) :
        TcpState(parent),
        _socket(-1) {}

    /**
     *  Constructor
     *  @param  state
     */
    TcpExtState(TcpExtState *state) :
        TcpState(state),
        _socket(state->_socket) 
    {
        // invalidate the other state
        state->_socket = -1;
    }
    
public:
    /**
     *  No copying
     *  @param  that
     */
    TcpExtState(const TcpExtState &that) = delete;
    
    /**
     *  Destructor
     */
    virtual ~TcpExtState()
    {
        // cleanup the socket
        cleanup();
    }

    /**
     *  The filedescriptor of this connection
     *  @return int
     */
    virtual int fileno() const override
    { 
        // expose the socket
        return _socket; 
    }
};

/**
 *  End of namespace
 */
}


