SELECT ascii('234');
SELECT ascii(materialize('234'));
SELECT ascii(toString(number) || 'abc') from numbers(10);
