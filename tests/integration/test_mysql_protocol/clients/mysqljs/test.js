var mysql      = require('mysql');

var connection = mysql.createConnection({
    host     : process.argv[2],
    port     : process.argv[3],
    user     : process.argv[4],
    password : process.argv[5],
    database : 'system',
});

connection.connect();

connection.query('SELECT 1 + 1 AS solution', function (error, results, fields) {
    if (error) throw error;

    if (results[0].solution.toString() !== '2') {
        throw Error('Wrong result of a query. Expected: "2", received: ' + results[0].solution + '.')
    }
});

connection.end();
