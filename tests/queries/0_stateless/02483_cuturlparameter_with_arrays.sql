SET enable_analyzer = 1;

-- { echoOn }

SELECT
    cutURLParameter('http://bigmir.net/?a=b&c=d', []),
    cutURLParameter('http://bigmir.net/?a=b&c=d', ['a']),
    cutURLParameter('http://bigmir.net/?a=b&c=d', ['a', 'c']),
    cutURLParameter('http://bigmir.net/?a=b&c=d', ['c']),
    cutURLParameter('http://bigmir.net/?a=b&c=d#e=f', ['a', 'e']),
    cutURLParameter('http://bigmir.net/?a&c=d#e=f', ['c', 'e']),
    cutURLParameter('http://bigmir.net/?a&c=d#e=f', ['e']),
    cutURLParameter('http://bigmir.net/?a=b&c=d#e=f&g=h', ['b', 'g']),
    cutURLParameter('http://bigmir.net/?a=b&c=d#e', ['a', 'e']),
    cutURLParameter('http://bigmir.net/?a=b&c=d#e&g=h', ['c', 'g']),
    cutURLParameter('http://bigmir.net/?a=b&c=d#e&g=h', ['e', 'g']),
    cutURLParameter('http://bigmir.net/?a=b&c=d#test?e=f&g=h', ['test', 'e']),
    cutURLParameter('http://bigmir.net/?a=b&c=d#test?e=f&g=h', ['test', 'g']),
    cutURLParameter('//bigmir.net/?a=b&c=d', []),
    cutURLParameter('//bigmir.net/?a=b&c=d', ['a']),
    cutURLParameter('//bigmir.net/?a=b&c=d', ['a', 'c']),
    cutURLParameter('//bigmir.net/?a=b&c=d#e=f', ['a', 'e']),
    cutURLParameter('//bigmir.net/?a&c=d#e=f', ['a']),
    cutURLParameter('//bigmir.net/?a&c=d#e=f', ['a', 'c']),
    cutURLParameter('//bigmir.net/?a&c=d#e=f', ['a', 'e']),
    cutURLParameter('//bigmir.net/?a=b&c=d#e=f&g=h', ['c', 'g']),
    cutURLParameter('//bigmir.net/?a=b&c=d#e', ['a', 'c']),
    cutURLParameter('//bigmir.net/?a=b&c=d#e', ['a', 'e']),
    cutURLParameter('//bigmir.net/?a=b&c=d#e&g=h', ['c', 'e']),
    cutURLParameter('//bigmir.net/?a=b&c=d#e&g=h', ['e', 'g']),
    cutURLParameter('//bigmir.net/?a=b&c=d#test?e=f&g=h', ['test', 'e']),
    cutURLParameter('//bigmir.net/?a=b&c=d#test?e=f&g=h', ['test', 'g'])
    FORMAT Vertical;

SELECT
    cutURLParameter(materialize('http://bigmir.net/?a=b&c=d'), []),
    cutURLParameter(materialize('http://bigmir.net/?a=b&c=d'), ['a']),
    cutURLParameter(materialize('http://bigmir.net/?a=b&c=d'), ['a', 'c']),
    cutURLParameter(materialize('http://bigmir.net/?a=b&c=d'), ['c']),
    cutURLParameter(materialize('http://bigmir.net/?a=b&c=d#e=f'), ['a', 'e']),
    cutURLParameter(materialize('http://bigmir.net/?a&c=d#e=f'), ['c', 'e']),
    cutURLParameter(materialize('http://bigmir.net/?a&c=d#e=f'), ['e']),
    cutURLParameter(materialize('http://bigmir.net/?a=b&c=d#e=f&g=h'), ['b', 'g']),
    cutURLParameter(materialize('http://bigmir.net/?a=b&c=d#e'), ['a', 'e']),
    cutURLParameter(materialize('http://bigmir.net/?a=b&c=d#e&g=h'), ['c', 'g']),
    cutURLParameter(materialize('http://bigmir.net/?a=b&c=d#e&g=h'), ['e', 'g']),
    cutURLParameter(materialize('http://bigmir.net/?a=b&c=d#test?e=f&g=h'), ['test', 'e']),
    cutURLParameter(materialize('http://bigmir.net/?a=b&c=d#test?e=f&g=h'), ['test', 'g']),
    cutURLParameter(materialize('//bigmir.net/?a=b&c=d'), []),
    cutURLParameter(materialize('//bigmir.net/?a=b&c=d'), ['a']),
    cutURLParameter(materialize('//bigmir.net/?a=b&c=d'), ['a', 'c']),
    cutURLParameter(materialize('//bigmir.net/?a=b&c=d#e=f'), ['a', 'e']),
    cutURLParameter(materialize('//bigmir.net/?a&c=d#e=f'), ['a']),
    cutURLParameter(materialize('//bigmir.net/?a&c=d#e=f'), ['a', 'c']),
    cutURLParameter(materialize('//bigmir.net/?a&c=d#e=f'), ['a', 'e']),
    cutURLParameter(materialize('//bigmir.net/?a=b&c=d#e=f&g=h'), ['c', 'g']),
    cutURLParameter(materialize('//bigmir.net/?a=b&c=d#e'), ['a', 'c']),
    cutURLParameter(materialize('//bigmir.net/?a=b&c=d#e'), ['a', 'e']),
    cutURLParameter(materialize('//bigmir.net/?a=b&c=d#e&g=h'), ['c', 'e']),
    cutURLParameter(materialize('//bigmir.net/?a=b&c=d#e&g=h'), ['e', 'g']),
    cutURLParameter(materialize('//bigmir.net/?a=b&c=d#test?e=f&g=h'), ['test', 'e']),
    cutURLParameter(materialize('//bigmir.net/?a=b&c=d#test?e=f&g=h'), ['test', 'g'])
    FORMAT Vertical;
-- { echoOff }
