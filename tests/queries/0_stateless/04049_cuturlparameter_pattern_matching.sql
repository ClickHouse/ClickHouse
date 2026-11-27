SELECT
    cutURLParameter('http://bigmir.net/?ba=1&a=2', 'a'),
    cutURLParameter('http://bigmir.net/?a=2&ba=1', 'a'),
    cutURLParameter('http://bigmir.net/?ba=1&a=2', ['a']),
    cutURLParameter('http://bigmir.net/?a=2&ba=1', ['a']),
    cutURLParameter('http://bigmir.net/?ba=1&a=2', ['a', 'ba'])
    FORMAT Vertical;