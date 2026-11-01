select * from format(Values, 'x DateTime64(3)', '(999999999999999999::Decimal64(0))'); -- { serverError DECIMAL_OVERFLOW }
