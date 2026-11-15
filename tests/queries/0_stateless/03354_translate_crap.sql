SELECT translate('aAbBcC', 'abc', toFixedString('12', 2)) AS a, toTypeName(a);
