# Отчёт: сравнение способов «естественной» сортировки строк

**Дата прогона:** 2026-02-10  
**Тест:** `tests/performance/natural_sort.xml`  
**Параметры:** `--runs 1`, полный прогон (40 запросов). Сервер из сборки с функцией `naturalSortKey`.

Сравниваются четыре варианта: обычный sort, **ORDER BY key NATURAL**, **ORDER BY key COLLATE 'en-u-kn-true'**, **ORDER BY naturalSortKey(key)**.

---

## Варианты сортировки

| Вариант | Описание |
|--------|----------|
| **ORDER BY key** | Обычная лексикографическая сортировка |
| **ORDER BY key NATURAL** | Встроенная естественная сортировка (sortBlock/SortCursor) |
| **ORDER BY key COLLATE 'en-u-kn-true'** | ICU collation с numeric ordering (`kn=true`) |
| **ORDER BY naturalSortKey(key)** | Сортировка по ключу функции `naturalSortKey` |

---

## Результаты (медиана времени запроса, секунды)

### Small (100K строк, LIMIT 10K)

| Вариант | Медиана (с) | Относительно обычного sort |
|---------|-------------|----------------------------|
| ORDER BY key | 0.00643 | 1.0× |
| ORDER BY key NATURAL | 0.00891 | ~1.39× |
| ORDER BY key COLLATE 'en-u-kn-true' | 0.0622 | **~9.7×** |
| ORDER BY naturalSortKey(key) | 0.00706 | ~1.10× |

### Medium (1M строк, LIMIT 100K)

| Вариант | Медиана (с) | Относительно обычного sort |
|---------|-------------|----------------------------|
| ORDER BY key | 0.0314 | 1.0× |
| ORDER BY key NATURAL | 0.0191 | **~0.61×** (NATURAL быстрее) |
| ORDER BY key COLLATE 'en-u-kn-true' | 0.727 | **~23×** |
| ORDER BY naturalSortKey(key) | 0.0309 | ~0.98× |

### Large (10M строк, LIMIT 1M)

| Вариант | Медиана (с) | Относительно обычного sort |
|---------|-------------|----------------------------|
| ORDER BY key | 0.250 | 1.0× |
| ORDER BY key NATURAL | 0.170 | **~0.68×** (NATURAL быстрее) |
| ORDER BY key COLLATE 'en-u-kn-true' | 8.49 | **~34×** |
| ORDER BY naturalSortKey(key) | 0.244 | ~0.98× |

### Small, полная сортировка (100K строк, без LIMIT)

| Вариант | Медиана (с) | Относительно обычного sort |
|---------|-------------|----------------------------|
| ORDER BY key | 0.0154 | 1.0× |
| ORDER BY key NATURAL | 0.00854 | **~0.55×** (NATURAL быстрее) |
| ORDER BY key COLLATE 'en-u-kn-true' | 0.303 | **~20×** |
| ORDER BY naturalSortKey(key) | 0.0153 | ~0.99× |

### Medium, полная сортировка (1M строк, без LIMIT)

| Вариант | Медиана (с) | Относительно обычного sort |
|---------|-------------|----------------------------|
| ORDER BY key | 0.0478 | 1.0× |
| ORDER BY key NATURAL | 0.0206 | **~0.43×** (NATURAL быстрее) |
| ORDER BY key COLLATE 'en-u-kn-true' | 0.767 | **~16×** |
| ORDER BY naturalSortKey(key) | 0.0299 | **~0.63×** (naturalSortKey быстрее) |

### Mixed (1M строк, смешанные паттерны, LIMIT 100K)

| Вариант | Медиана (с) | Относительно обычного sort |
|---------|-------------|----------------------------|
| ORDER BY key | 0.0362 | 1.0× |
| ORDER BY key NATURAL | 0.114 | ~3.2× |
| ORDER BY key COLLATE 'en-u-kn-true' | 0.571 | **~16×** |
| ORDER BY naturalSortKey(key) | 0.0422 | ~1.17× |

### Files (1M строк, file1..fileN, LIMIT 100K)

| Вариант | Медиана (с) | Относительно обычного sort |
|---------|-------------|----------------------------|
| ORDER BY key | 0.0306 | 1.0× |
| ORDER BY key NATURAL | 0.0197 | **~0.64×** (NATURAL быстрее) |
| ORDER BY key COLLATE 'en-u-kn-true' | 0.848 | **~28×** |
| ORDER BY naturalSortKey(key) | 0.0323 | ~1.06× |

### Versions (1M строк, LIMIT 100K)

| Вариант | Медиана (с) | Относительно обычного sort |
|---------|-------------|----------------------------|
| ORDER BY key | 0.0686 | 1.0× |
| ORDER BY key NATURAL | 0.174 | ~2.5× |
| ORDER BY key COLLATE 'en-u-kn-true' | 0.690 | **~10×** |
| ORDER BY naturalSortKey(key) | 0.0750 | ~1.09× |

### DESC (medium, 1M строк, LIMIT 100K)

| Вариант | Медиана (с) | Относительно обычного sort |
|---------|-------------|----------------------------|
| ORDER BY key DESC | 0.0428 | 1.0× |
| ORDER BY key DESC NATURAL | 0.0314 | **~0.73×** |
| ORDER BY key DESC COLLATE 'en-u-kn-true' | 1.047 | **~24×** |
| ORDER BY naturalSortKey(key) DESC | 0.0395 | **~0.92×** (naturalSortKey быстрее) |

### Multi-column (medium, ORDER BY first key, then len)

| Вариант | Медиана (с) | Относительно обычного sort |
|---------|-------------|----------------------------|
| ORDER BY key, len | 0.0421 | 1.0× |
| ORDER BY key NATURAL, len | 0.0292 | **~0.69×** (NATURAL быстрее) |
| ORDER BY key COLLATE 'en-u-kn-true', len | 1.060 | **~25×** |
| ORDER BY naturalSortKey(key), len | 0.0433 | ~1.03× |