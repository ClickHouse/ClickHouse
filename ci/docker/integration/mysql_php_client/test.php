<?php
$host = $argv[1];
$db   = "system";
$user = $argv[3];
$pass = $argv[4];
$charset = "utf8mb4";
$port = $argv[2];

$dsn = "mysql:host=$host;port=$port;dbname=$db;charset=$charset";
$options = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
    PDO::MYSQL_ATTR_DIRECT_QUERY => true,
];
$pdo = new PDO($dsn, $user, $pass, $options);

$stmt = $pdo->query("SELECT name FROM tables WHERE name = 'tables'");

foreach ($stmt as $row)
{
    echo $row["name"] . "\n";
}
?>
