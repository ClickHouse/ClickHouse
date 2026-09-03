SELECT ascii('234');
SELECT ascii('');
SELECT ascii(materialize('234'));
SELECT ascii(materialize(''));
SELECT ascii(toString(number) || 'abc') from numbers(10);
