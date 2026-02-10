# Сравнение ORDER BY key NATURAL и ORDER BY naturalSortKey(key)

Отдельный тест без COLLATE. Увеличенные объёмы (2M, 10M, 30M строк), полная сортировка — чтобы разница между NATURAL и naturalSortKey была наглядной.

**Тест:** `natural_vs_natural_sort_key.xml`  
**Параметры:** `--runs 1`.  
**Дата прогона:** 2026-02-10.

---

## Варианты (только три)

| Вариант | Описание |
|--------|----------|
| **ORDER BY key** | Обычная лексикографическая сортировка (базовый уровень) |
| **ORDER BY key NATURAL** | Встроенная естественная сортировка (sortBlock/SortCursor) |
| **ORDER BY naturalSortKey(key)** | Сортировка по ключу функции `naturalSortKey` |

---

## Результаты (медиана времени, с)

### 2M строк, полная сортировка

| Вариант | Медиана (с) | Относительно key | NATURAL vs naturalSortKey |
|---------|-------------|------------------|---------------------------|
| ORDER BY key | 0.0971 | 1.0× | — |
| ORDER BY key NATURAL | 0.0363 | **0.37×** (быстрее) | **NATURAL ~1.64× быстрее** |
| ORDER BY naturalSortKey(key) | 0.0596 | 0.61× | — |

### 10M строк, полная сортировка

| Вариант | Медиана (с) | Относительно key | NATURAL vs naturalSortKey |
|---------|-------------|------------------|---------------------------|
| ORDER BY key | 0.453 | 1.0× | — |
| ORDER BY key NATURAL | 0.177 | **0.39×** (быстрее) | **NATURAL ~1.56× быстрее** |
| ORDER BY naturalSortKey(key) | 0.277 | 0.61× | — |

### 30M строк, полная сортировка

| Вариант | Медиана (с) | Относительно key | NATURAL vs naturalSortKey |
|---------|-------------|------------------|---------------------------|
| ORDER BY key | 1.444 | 1.0× | — |
| ORDER BY key NATURAL | 0.537 | **0.37×** (быстрее) | **NATURAL ~1.61× быстрее** |
| ORDER BY naturalSortKey(key) | 0.863 | 0.60× | — |

---

## Вывод: что лучше — NATURAL или naturalSortKey

- **ORDER BY key NATURAL** при этих объёмах стабильно быстрее **ORDER BY naturalSortKey(key)** примерно в **1.5–1.6 раза** (2M: 36 ms vs 60 ms, 10M: 177 ms vs 277 ms, 30M: 537 ms vs 863 ms).
- Оба варианта быстрее обычного **ORDER BY key** (NATURAL ~2.5–2.7×, naturalSortKey ~1.6×).
- Для естественной сортировки по колонке выгоднее использовать **ORDER BY key NATURAL**, чем **ORDER BY naturalSortKey(key)**.

---

## Как перезапустить тест

```bash
cd tests/performance
python3 scripts/perf.py --runs 1 natural_vs_natural_sort_key.xml
```

Сервер должен быть собран с функцией `naturalSortKey` (например, `build/programs/clickhouse`).
