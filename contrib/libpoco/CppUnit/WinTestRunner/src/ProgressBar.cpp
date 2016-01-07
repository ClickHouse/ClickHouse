//
// ProgressBar.cpp
//
// $Id: //poco/1.4/CppUnit/WinTestRunner/src/ProgressBar.cpp#1 $
//


#include "ProgressBar.h"


namespace CppUnit {


// Paint the progress bar in response to a paint message
void ProgressBar::paint(CDC& dc)
{
    paintBackground (dc);
    paintStatus (dc);
}


// Paint the background of the progress bar region
void ProgressBar::paintBackground (CDC& dc)
{
    CBrush      brshBackground;
    CPen        penGray     (PS_SOLID, 1, RGB (128, 128, 128));
    CPen        penWhite    (PS_SOLID, 1, RGB (255, 255, 255));

    VERIFY (brshBackground.CreateSolidBrush (::GetSysColor (COLOR_BTNFACE)));

    dc.FillRect (_bounds, &brshBackground);

    CPen    *pOldPen;

    pOldPen = dc.SelectObject (&penGray);
    {
        dc.MoveTo (_bounds.left, _bounds.top);
        dc.LineTo (_bounds.left + _bounds.Width () -1, _bounds.top);

        dc.MoveTo (_bounds.left, _bounds.top);
        dc.LineTo (_bounds.left, _bounds.top + _bounds.Height () -1);

    }
    dc.SelectObject (&penWhite);
    {
        dc.MoveTo (_bounds.left + _bounds.Width () -1, _bounds.top);
        dc.LineTo (_bounds.left + _bounds.Width () -1, _bounds.top + _bounds.Height () -1);

        dc.MoveTo (_bounds.left, _bounds.top + _bounds.Height () -1);
        dc.LineTo (_bounds.left + _bounds.Width () -1, _bounds.top + _bounds.Height () -1);

    }
    dc.SelectObject (pOldPen);

}


// Paint the actual status of the progress bar
void ProgressBar::paintStatus (CDC& dc)
{
    if (_progress <= 0)
        return;

    CBrush      brshStatus;
    CRect       rect (_bounds.left, _bounds.top,
                    _bounds.left + _progressX, _bounds.bottom);

    COLORREF    statusColor = getStatusColor ();

    VERIFY (brshStatus.CreateSolidBrush (statusColor));

    rect.DeflateRect (1, 1);
    dc.FillRect (rect, &brshStatus);

}


// Paint the current step
void ProgressBar::paintStep (int startX, int endX)
{
    // kludge: painting the whole region on each step
    _baseWindow->RedrawWindow (_bounds);
    _baseWindow->UpdateWindow ();

}


// Setup the progress bar for execution over a total number of steps
void ProgressBar::start (int total)
{
    _total = total;
    reset ();
}


// Take one step, indicating whether it was a successful step
void ProgressBar::step (bool successful)
{
    _progress++;

    int x = _progressX;

    _progressX = scale (_progress);

    if (!_error && !successful)
    {
        _error = true;
        x = 1;
    }

    paintStep (x, _progressX);

}


// Map from steps to display units
int ProgressBar::scale (int value)
{
    if (_total > 0)
        return max (1, value * (_bounds.Width () - 1) / _total);

    return value;

}


// Reset the progress bar
void ProgressBar::reset ()
{
    _progressX     = 1;
    _progress      = 0;
    _error         = false;

    _baseWindow->RedrawWindow (_bounds);
    _baseWindow->UpdateWindow ();

}


} // namespace CppUnit
