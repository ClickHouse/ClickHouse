# PrettyNoEscapes

Отличается от Pretty тем, что не используются ANSI-escape последовательности. Это нужно для отображения этого формата в браузере, а также при использовании утилиты командной строки watch.

Пример:

```bash
watch -n1 "clickhouse-client --query='SELECT * FROM system.events FORMAT PrettyCompactNoEscapes'"
```

Для отображения в браузере, вы можете использовать HTTP интерфейс.

## PrettyCompactNoEscapes

Аналогично.

## PrettySpaceNoEscapes

Аналогично.
