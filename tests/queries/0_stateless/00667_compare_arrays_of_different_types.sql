SELECT
    [1] < [1000],
    ['abc'] = [NULL],
    ['abc'] = [toNullable('abc')],
    [[]] = [[]],
    [[], [1]] > [[], []],
    [[1]] < [[], []],
    [[], []] > [[]],
    [([], ([], []))] < [([], ([], ['hello']))]
;

SELECT
    materialize([1]) < materialize([1000]),
    materialize(['abc']) = materialize([NULL]),
    materialize(['abc']) = materialize([toNullable('abc')]),
    materialize([[]]) = materialize([[]]),
    materialize([[], [1]]) > materialize([[], []]),
    materialize([[1]]) < materialize([[], []]),
    materialize([[], []]) > materialize([[]]),
    materialize([([], ([], []))]) < materialize([([], ([], ['hello']))])
;
