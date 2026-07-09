---
description: 'Обзор системы непрерывной интеграции ClickHouse'
sidebar_label: 'Непрерывная интеграция (CI)'
sidebar_position: 55
slug: /development/continuous-integration
title: 'Непрерывная интеграция (CI)'
doc_type: 'reference'
---

Когда вы отправляете pull request, система [непрерывной интеграции (CI)](tests.md#test-automation) ClickHouse запускает для вашего кода ряд автоматических проверок.
Это происходит после того, как сопровождающий репозитория (кто-то из ClickHouse team) просмотрит ваш код и добавит к вашему pull request метку `can be tested`.
Результаты проверок отображаются на странице pull request в GitHub, как описано в [документации GitHub по проверкам](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-status-checks).
Если какая-либо проверка завершается с ошибкой, вам может потребоваться её исправить.
На этой странице дан обзор проверок, с которыми вы можете столкнуться, и способов их исправления.

Если похоже, что сбой проверки не связан с вашими изменениями, это может быть временный сбой или проблема с инфраструктурой.
Отправьте в pull request пустой коммит, чтобы перезапустить проверки CI:

```shell
git commit --allow-empty
git push
```

Если вы не уверены, что делать, обратитесь за помощью к мейнтейнеру.

<div id="merge-with-master">
  ## Слияние с master
</div>

Проверяет, что PR можно влить в master.
Если нет, проверка завершится с сообщением `Cannot fetch mergecommit`.
Чтобы пройти эту проверку, разрешите конфликт, как описано в [документации GitHub](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/resolving-a-merge-conflict-on-github), или влейте ветку `master` в ветку вашего pull request с помощью git.

<div id="docs-check">
  ## Проверка документации
</div>

Проверяет, собирается ли сайт документации ClickHouse.
Проверка может завершиться ошибкой, если вы что-то изменили в документации.
Наиболее вероятная причина — некорректная перекрестная ссылка в документации.
Перейдите к отчету проверки и найдите сообщения `ERROR` и `WARNING`.

<div id="description-check">
  ## Проверка описания
</div>

Убедитесь, что описание вашего pull request соответствует шаблону [PULL&#95;REQUEST&#95;TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md).
Вы должны указать категорию changelog для вашего изменения (например, исправление ошибки) и написать понятное пользователю сообщение, описывающее это изменение, для [CHANGELOG.md](../whats-new/changelog/index.md)

<div id="docker-image">
  ## Docker-образ
</div>

Собирает Docker-образы сервера ClickHouse и Keeper, чтобы убедиться, что они собираются правильно.

<div id="official-docker-library-tests">
  ### Официальные тесты библиотеки Docker
</div>

Запускает тесты из [официальной библиотеки Docker](https://github.com/docker-library/official-images/tree/master/test#alternate-config-files), чтобы проверить, что Docker-образ `clickhouse/clickhouse-server` работает корректно.

Чтобы добавить новые тесты, создайте каталог `ci/jobs/scripts/docker_server/tests/$test_name` и поместите в него script `run.sh`.

Дополнительные сведения о тестах см. в [документации scripts задач CI](https://github.com/ClickHouse/ClickHouse/tree/master/ci/jobs/scripts/docker_server).

<div id="marker-check">
  ## Проверка Marker
</div>

Эта проверка означает, что система CI начала обработку pull request.
Если у неё статус &#39;pending&#39;, это значит, что запущены ещё не все проверки.
После запуска всех проверок её статус меняется на &#39;success&#39;.

<div id="style-check">
  ## Style check
</div>

Выполняет различные проверки стиля в кодовой базе. Каждая из перечисленных ниже подпроверок соответствует `testname` в [`ci/jobs/check_style.py`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/check_style.py) и может быть запущена отдельно с помощью `--test <name>` (см. ниже).

<div id="cpp">
  ##### cpp
</div>

Проверка стиля C++ на основе Regex с помощью [`check_cpp.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/check_cpp.sh). Если проверка завершается с ошибкой, исправьте проблемы в соответствии с [руководством по стилю кода](style.md).

<div id="whitespace-check">
  ##### whitespace_check
</div>

Выявляет двойные пробелы после запятых в C++, не связанные с выравниванием столбцов.

<div id="catch-all">
  ##### catch_all
</div>

Запрещает `catch (...)` вне деструкторов, `main` и точек входа фаззера, где перехват с проглатыванием неизвестного исключения небезопасен.

<div id="yamllint">
  ##### yamllint
</div>

Проверяет YAML-файлы workflow в `.github/` с помощью `.yamllint`.

<div id="xmllint">
  ##### xmllint
</div>

Проверяет XML-файлы в каталогах `tests/` и `programs/`.

<div id="functional-tests-check">
  ##### functional_tests_check
</div>

Проверяет тесты без сохранения состояния: в запросах с фильтрацией по `event_date` следует использовать `>= yesterday()`, а не `today()` (чтобы избежать нестабильного поведения около полуночи), а имена файлов тестов не должны содержать `fail`.

<div id="test-numbers-check">
  ##### test_numbers_check
</div>

Выявляет большие пропуски в нумерации тестов без сохранения состояния (`tests/queries/0_stateless/<NNNNN>_*`).

<div id="symlinks">
  ##### символические ссылки
</div>

Обнаруживает битые символические ссылки в репозитории.

<div id="various">
  ##### разное
</div>

Разные проверки репозитория с помощью [`various_checks.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/various_checks.sh): запросы к `system.query_log` / `system.parts` / и т. д. должны фильтроваться по `currentDatabase`, пути ZooKeeper для `Replicated*MergeTree` должны включать отдельный префикс для каждого теста, каталоги интеграционных тестов должны содержать `__init__.py`, UTF BOM не допускаются, у файлов с исходным кодом и файлов данных не должны быть выставлены биты executable, у сторонних образов в docker-compose не должно быть тегов `:latest`, и многое другое.

<div id="running-style-check-locally">
  ### Локальный запуск задачи *Style Check*
</div>

Всю задачу *Style Check* можно запустить локально в контейнере Docker с помощью:

```sh
python -m ci.praktika run "Style check"
```

Чтобы запустить определённую проверку (например, проверку *cpp*):

```sh
python -m ci.praktika run "Style check" --test cpp
```

Эти команды загружают Docker-образ `clickhouse/style-test` и запускают задачу в контейнерной среде.
Никаких зависимостей, кроме Python 3 и Docker, не требуется.

<div id="running-stateless-tests">
  ## Запуск тестов без сохранения состояния
</div>

Локально установленный ClickHouse с настройками по умолчанию может подойти для отдельных тестовых сценариев, но не позволяет корректно выполнять все тестовые запросы. В CI для каждой задачи применяется определённая конфигурация ClickHouse (например, хранилище S3, параллельные реплики), и вручную воспроизвести её бывает затруднительно. Чтобы этого избежать, вы можете локально воспроизвести любую задачу CI, используя ту же оркестрацию, что и в CI, — без ручной настройки.

<div id="ci-prerequisites">
  #### Необходимые условия
</div>

* Python 3 (только стандартная библиотека)
* Docker

При необходимости установите Docker в Ubuntu и войдите в систему заново:

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "ipv6": true,
  "ip6tables": true
}
EOF
sudo systemctl restart docker
```

<div id="run-ci-job-locally">
  #### Запустите задачу CI локально
</div>

Выберите имя любой задачи из отчёта CI и запустите её локально:

```bash
python -m ci.praktika run "<JOB_NAME>"
```

* Всегда указывайте имя задачи точно так, как оно указано в отчёте CI (оно может содержать пробелы и запятые), например: `"Stateless tests (amd_debug, parallel)"`. Это задаст ту же конфигурацию ClickHouse и запустит те же тесты, что и в CI.
* Архитектура и тип сборки в имени задачи (например, `amd_debug`) — это специфичные для CI метки. При локальном запуске они ни на что не влияют: задача будет использовать предоставленный вами бинарный файл на той архитектуре, на которой вы запускаете её. Имя задачи определяет только конфигурацию ClickHouse и набор тестов (если это не переопределено через `--test`).
* В CI функциональные тесты разделены на батчи для более эффективного использования ресурсов. Например, `"Stateless tests (amd_debug, parallel)"` и `"Stateless tests (amd_debug, sequential)"` вместе охватывают весь набор: тесты, безопасные для параллельного выполнения, запускаются одновременно, а остальные — последовательно. Такое разделение сокращает общее время выполнения в CI, максимально используя параллелизм там, где это возможно. Чтобы локально воспроизвести полный набор тестов, запустите оба батча.
* Также есть задача CI `"Fast test"`, которая запускает ограниченный набор функциональных тестов для проверки базовой работоспособности ClickHouse — она использует сборку не со всеми дополнительными модулями и позволяет быстрее всего выявить регрессии. Её можно запустить локально таким же способом. Поместите бинарный файл ClickHouse в один из путей поиска по умолчанию (`./ci/tmp/clickhouse`, `./build/programs/clickhouse` или `./clickhouse`) — иначе задача сначала попытается собрать ClickHouse:
  ```bash
  python -m ci.praktika run "Fast test"
  ```

<div id="run-specific-tests-within-ci-job">
  #### Запуск отдельных тестов в рамках задачи CI
</div>

С флагом `--test` задача подготавливает такую же конфигурацию ClickHouse, как и в CI, но запускает только выбранные тесты:

```bash
python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
  --test 00001_select1
```

* Можно передать несколько названий тестов:
  ```bash
  python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
    --test 00001_select1 00002_log_and_exception_messages_formatting
  ```
* Совет: если подходит любая конфигурация ClickHouse и нужно запустить только определённые тесты, используйте псевдоним `functional` вместо полного имени задачи:
  ```bash
  python -m ci.praktika run functional --test 00001_select1
  ```

<div id="additional-customization-options">
  #### Дополнительные параметры настройки
</div>

* `--path PATH` — путь к бинарному файлу ClickHouse, заданный вручную. По умолчанию раннер ищет его в следующем порядке: `./ci/tmp/clickhouse`, `./build/programs/clickhouse`, `./clickhouse`.
* `--count N` — повторить каждый тест N раз.
* `--workers N` — переопределить автоматически вычисляемое число параллельных воркеров исходя из ресурсов машины.

<div id="build-check">
  ## Проверка сборки
</div>

Выполняет сборку ClickHouse в различных конфигурациях для использования на следующих этапах.

<div id="running-builds-locally">
  ### Локальный запуск сборок
</div>

Сборки можно запускать локально в среде, аналогичной CI, с помощью:

```bash
python -m ci.praktika run "<BUILD_JOB_NAME>"
```

Никаких зависимостей, кроме Python 3 и Docker, не требуется.

<div id="available-build-jobs">
  #### Доступные задачи сборки
</div>

Названия задач сборки в точности совпадают с тем, как они указаны в отчёте CI:

**Сборки AMD64:**

* `Build (amd_debug)` - Отладочная сборка с символами
* `Build (amd_release)` - Оптимизированная релизная сборка
* `Build (amd_asan)` - Сборка с Address Sanitizer
* `Build (amd_tsan)` - Сборка с Thread Sanitizer
* `Build (amd_msan)` - Сборка с Memory Sanitizer
* `Build (amd_ubsan)` - Сборка с Undefined Behavior Sanitizer
* `Build (amd_binary)` - Быстрая релизная сборка без Thin LTO
* `Build (amd_compat)` - Сборка для совместимости со старыми системами
* `Build (amd_musl)` - Сборка с musl libc
* `Build (amd_darwin)` - Сборка для macOS
* `Build (amd_freebsd)` - Сборка для FreeBSD

**Сборки ARM64:**

* `Build (arm_release)` - Оптимизированная релизная сборка для ARM64
* `Build (arm_asan)` - Сборка с Address Sanitizer для ARM64
* `Build (arm_coverage)` - Сборка для ARM64 с инструментированием покрытия
* `Build (arm_binary)` - Быстрая релизная сборка для ARM64 без Thin LTO
* `Build (arm_darwin)` - Сборка для macOS на ARM64
* `Build (arm_v80compat)` - Сборка для совместимости с ARMv8.0

**Другие архитектуры:**

* `Build (ppc64le)` - PowerPC, 64-бит, little-endian
* `Build (riscv64)` - RISC-V, 64-бит
* `Build (s390x)` - IBM System/390, 64-бит
* `Build (loongarch64)` - LoongArch, 64-бит

Если задача завершится успешно, результаты сборки будут доступны в каталоге `<repo_root>/ci/tmp/build`.

**Note:** Для сборок вне категории &quot;Другие архитектуры&quot; (в них используется кросс-компиляция) архитектура локальной машины должна соответствовать типу сборки, чтобы получить сборку, запрошенную через `BUILD_JOB_NAME`.

<div id="example-run-local">
  #### Пример
</div>

Для запуска локальной отладочной сборки:

```bash
python -m ci.praktika run "Build (amd_debug)"
```

Если описанный выше подход вам не подходит, используйте параметры cmake из журнала сборки и следуйте [общему процессу сборки](../development/build.md).

<div id="functional-stateless-tests">
  ## Функциональные тесты без сохранения состояния
</div>

Запускает [функциональные тесты без сохранения состояния](tests.md#functional-tests) для бинарных файлов ClickHouse, собранных в различных конфигурациях — release, debug, с санитайзерами и т. д.
Посмотрите отчет, чтобы узнать, какие тесты не проходят, а затем воспроизведите проблему локально, как описано [здесь](/ru/development/tests#functional-tests).
Обратите внимание: для воспроизведения нужно использовать правильную конфигурацию сборки — тест может падать при AddressSanitizer, но проходить в Debug.
Скачайте бинарный файл со [страницы проверок сборки в CI](/ru/install/advanced) или соберите его локально.

<div id="integration-tests">
  ## Интеграционные тесты
</div>

Запускает [интеграционные тесты](tests.md#integration-tests).

<div id="bugfix-validate-check">
  ## Проверка bugfix validate check
</div>

Проверяет, что есть либо новый тест (функциональный или интеграционный), либо изменённые тесты, которые падают при запуске с бинарным файлом, собранным из ветки master.
Эта проверка запускается, если у pull request есть метка &quot;pr-bugfix&quot;.

<div id="stress-test">
  ## Стресс-тест
</div>

Запускает функциональные тесты без сохранения состояния одновременно от нескольких клиентов, чтобы выявить ошибки, связанные с параллелизмом. Если он завершается сбоем:

* Сначала устраните все остальные сбои тестов;
  * Просмотрите отчёт, найдите серверные журналы и проверьте их на наличие возможных причин
    ошибки.

<div id="compatibility-check">
  ## Проверка совместимости
</div>

Проверяет, запускается ли бинарный файл `clickhouse` в дистрибутивах со старыми версиями libc.
Если проверка не проходит, обратитесь за помощью к мейнтейнеру.

<div id="ast-fuzzer">
  ## AST-фаззер
</div>

Запускает случайно сгенерированные запросы, чтобы выявлять ошибки в программе.
Если он завершится с ошибкой, обратитесь за помощью к мейнтейнеру.

<div id="performance-tests">
  ## Тесты производительности
</div>

Позволяют измерять изменения в производительности запросов.
Это самая длительная проверка, выполнение которой занимает чуть менее 6 часов.
Подробное описание отчёта о тесте производительности приведено [здесь](https://github.com/ClickHouse/ClickHouse/blob/master/tests/performance/scripts/README.md#how-to-read-the-report).