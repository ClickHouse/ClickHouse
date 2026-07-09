---
description: 'Descripción general de la política de backports y de la automatización de ClickHouse'
sidebar_label: 'Sistema de backports'
sidebar_position: 56
slug: /development/backports
title: 'Sistema de backports'
doc_type: 'reference'
---

Este documento describe la política de backports de ClickHouse y el sistema automatizado que la implementa.

<div id="release-model">
  ## Modelo de versiones
</div>

Las versiones de ClickHouse siguen el esquema `YY.M.patch.build-type`, donde `YY` es el año con dos dígitos, `M` es el mes de la versión (sin cero inicial), `patch` es el número de parche dentro de la rama, `build` es un número de compilación que aumenta de forma monotónica y `type` es `stable` o `lts`.

Ejemplo: `25.3.8.23-lts` — LTS de marzo de 2025, parche 8, compilación 23.

Hay dos líneas de versiones:

* Las versiones **Stable** se publican aproximadamente una vez al mes. Las tres versiones estables más recientes reciben parches, lo que proporciona aproximadamente tres meses de soporte activo por versión.
* Las versiones **LTS (Long-Term Support)** se publican en marzo y agosto de cada año. Se mantienen dos versiones LTS de forma simultánea, cada una durante al menos 12 meses.

Se recomienda a los usuarios que ejecutan cargas de trabajo de producción que usen la versión estable más reciente o una versión LTS, y que actualicen cuanto antes a nuevas versiones de parche, ya que las versiones de parche no introducen cambios incompatibles.

<div id="backport-policy">
  ## Política de backport
</div>

No todos los cambios se aplican mediante backport. El objetivo es mantener estables las ramas de release, por lo que el alcance de los backports es deliberadamente limitado:

* **Correcciones de seguridad** — siempre se aplican mediante backport.
* **Correcciones críticas de errores** (exceptions (logical errors), pérdida de datos, resultados incorrectos, problemas de RBAC) — se seleccionan automáticamente para backport según las reglas generales de backport; se identifican con la label `pr-critical-bugfix`, que hace que `pr-must-backport` se añada automáticamente.
* **Correcciones de estabilidad y regresiones** — se aplican mediante backport cuando el riesgo del cambio es bajo en comparación con el riesgo de dejar el error sin corregir; se identifican mediante `pr-must-backport`, añadida manualmente por los maintainers.
* **Correcciones menores de errores con una solución alternativa disponible** — por lo general, no se aplican mediante backport para evitar desestabilizar las ramas de release.
* **Nuevas funcionalidades, mejoras y optimizaciones de rendimiento** — no se aplican mediante backport.

La label `pr-must-backport` es la sobrescritura manual que usan los maintainers para marcar un PR para backport. La label `pr-critical-bugfix` hace que `pr-must-backport` se añada automáticamente mediante el hook de CI (consulta `pr_labels_and_category.py`).

**Escalación de conflictos.** Cuando el backport automático no puede resolver conflictos de merge, igualmente debe crearse un cherry-pick PR y asignarse al author, a quien hizo el merge y a los assignees existentes del PR original para que una persona pueda resolver los conflictos y completar el backport.

<div id="backport-tool">
  ## Herramienta de backport
</div>

La política de backport descrita anteriormente está implementada por la herramienta automatizada en `tests/ci/cherry_pick.py`. La herramienta se ejecuta como un workflow de GitHub Actions en la infraestructura de ClickHouse y cubre todos los requisitos: detectar ramas de lanzamiento activas, seleccionar las PR aptas para backport, realizar el procedimiento de cherry-pick y backport en dos etapas, gestionar conflictos, aplicar la política de demora y mantener las etiquetas sincronizadas.

El objetivo a largo plazo es extraer esta implementación y convertirla en una herramienta independiente de Python de código abierto que otros proyectos puedan adoptar. El diseño previsto es:

* **Configurable** — todos los parámetros de la política (etiquetas aptas, ventana de demora, umbrales para PR obsoletas, comportamiento durante el rolling-out, etc.) se expresan en un archivo de configuración para que la herramienta pueda adaptarse a los requisitos de backport de cualquier proyecto sin cambios en el código.
* **Distribuible** — empaquetada como una wheel de Python autocontenida e instalable desde PyPI, sin depender de la infraestructura de CI de ClickHouse.
* **Programable** — expone un modelo de objetos claro para pull requests, etiquetas y ramas de lanzamiento, de modo que los usuarios puedan crear workflows personalizados sobre el motor principal.

<div id="testing">
  ### Pruebas
</div>

Una parte prevista de la herramienta standalone es una suite de pruebas específica junto con una infraestructura de pruebas ligera. La infraestructura podrá crear repositorios temporales de GitHub (o equivalentes locales) preparados con antelación con:

* un conjunto configurable de ramas que representan líneas de lanzamiento,
* pull requests con distintas combinaciones de etiquetas de backport,
* PR de lanzamiento con la etiqueta `release` que apuntan a las ramas de lanzamiento.

Esto permite que las pruebas ejerciten el ciclo completo de automatización —detección de etiquetas, creación de ramas de cherry-pick, gestión de conflictos, creación de pull requests de backport, lógica de asignación de responsables, omisión durante rolling-out y política de retraso— en un repositorio real pero desechable, sin afectar al estado de producción. La misma infraestructura puede reutilizarse para realizar pruebas de regresión de cambios de políticas antes de desplegarlos.

<div id="active-release-branches">
  ## Ramas de lanzamiento activas
</div>

Una rama de lanzamiento activa es cualquier rama cuyo PR de lanzamiento correspondiente (con la etiqueta `release`) sigue abierto en GitHub. La automatización de backport las detecta dinámicamente en cada ejecución, por lo que no hace falta cambiar la configuración cuando se crea una nueva release o una antigua llega al fin de su vida útil.

Una rama de lanzamiento puede estar en estado **rolling-out** (su PR de lanzamiento lleva la etiqueta `rolling-out`) durante el período en que se está desplegando una nueva release. Los backports generales se pausan para las ramas en rolling-out para no complicar el rollout. Las etiquetas específicas de versión (p. ej., `v25.3-must-backport`) prevalecen sobre esto y fuerzan el backport incluso durante un rollout.

Una etiqueta específica de versión establece la release *más antigua* que debe alcanzar el PR: se aplica mediante backport a esa release **y a todas las ramas de lanzamiento activas posteriores**, no solo a la indicada. Por ejemplo, `v25.3-must-backport` en un PR integrado en la rama de desarrollo se aplica mediante backport a `25.3` y a todas las releases activas posteriores (`25.4`, `25.5`, …). Si hay varias etiquetas específicas de versión, prevalece la versión más baja, ya que ya cubre las más nuevas.

La release indicada no tiene que estar activa por sí misma. Una etiqueta para una release en fin de su vida útil (una sin ningún PR de lanzamiento abierto) sigue propagando la corrección hacia adelante a todas las releases activas posteriores, de modo que al actualizar desde esa release la corrección nunca se pierda silenciosamente. Por ejemplo, `v25.12-must-backport` en un PR sigue aplicando backport a `26.1`, `26.2`, … incluso después de que `25.12` en sí haya llegado al fin de su vida útil.

<div id="implementation">
  ## Implementación
</div>

<div id="overview">
  ### Descripción general
</div>

La automatización de backport se ejecuta cada hora como el workflow `CherryPick` de GitHub Actions (`.github/workflows/cherry_pick.yml`), implementado en `tests/ci/cherry_pick.py`. Funciona mediante la API de GitHub y operaciones locales de git en un runner `style-checker-aarch64` self-hosted.

El proceso consta de dos etapas para cada par (PR original, rama de lanzamiento):

1. Se crea una **PR de cherry-pick** para aislar la resolución de conflictos del destino real del merge. Si no hay conflictos, se hace merge automáticamente.
2. Se crea una **PR de backport** sobre la rama de lanzamiento real, con los cambios de cherry-pick combinados en un único commit.

<div id="labels">
  ### Etiquetas
</div>

Las etiquetas del PR original determinan si se realiza el backport y en qué ramas.

| Etiqueta                                              | Efecto                                                                                                                                                                                                                                                                                                                                                                |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pr-must-backport`                                    | Backport a todas las ramas de lanzamiento activas (omitiendo las ramas marcadas como `rolling-out`)                                                                                                                                                                                                                                                                  |
| `pr-must-backport-force`                              | Backport a todas las ramas de lanzamiento activas, ignorando las restricciones de `rolling-out`                                                                                                                                                                                                                                                                      |
| `pr-critical-bugfix`                                  | Activa `pr-must-backport` automáticamente (mediante `AUTO_BACKPORT` en `pr_labels_and_category.py`)                                                                                                                                                                                                                                                                   |
| `v{VER}-must-backport` (p. ej. `v25.3-must-backport`) | Backport a esa rama de lanzamiento **y a todas las ramas de lanzamiento activas más recientes** — la versión marca la *release* más antigua a la que debe llegar el PR, incluso cuando la release indicada ya está en fin de su vida útil. Si hay varias etiquetas de este tipo, prevalece la versión más baja. Anula la omisión por `rolling-out` para esas ramas |
| `pr-backports-created`                                | La establece el bot cuando se han creado todos los PR de backport necesarios; se borra si se vuelve a abrir un PR de cherry-pick                                                                                                                                                                                                                                      |
| `pr-cherrypick`                                       | Se aplica a los PR de cherry-pick creados por el bot                                                                                                                                                                                                                                                                                                                  |
| `pr-backport`                                         | Se aplica a los PR de backport creados por el bot                                                                                                                                                                                                                                                                                                                     |
| `do not test`                                         | Se aplica a los PR de cherry-pick para que CI no se ejecute en ellos                                                                                                                                                                                                                                                                                                  |
| `rolling-out`                                         | Se establece en un **PR de lanzamiento** para indicar que su rama se está desplegando en ese momento; los backports generales la omiten                                                                                                                                                                                                                              |

<div id="branch-and-pr-naming">
  ### Nomenclatura de ramas y PR
</div>

Para cada PR original con número `N` y la rama de lanzamiento `release/X.Y`:

* Rama de cherry-pick: `cherrypick/release/X.Y/N`
* Rama de backport: `backport/release/X.Y/N`
* Título del PR de cherry-pick: `Cherry pick #N to release/X.Y: <original title>`
* Título del PR de backport: `Backport #N to release/X.Y: <original title>`

<div id="step-by-step-process">
  ### Proceso paso a paso
</div>

<div id="discover-active-releases">
  #### 1. Descubrir las versiones activas
</div>

`BackportPRs.receive_release_prs` consulta GitHub para obtener todos los PR abiertos con la etiqueta `release`. Las referencias `head` de estos PR son los nombres de las ramas de lanzamiento (p. ej., `release/25.3`). A partir de ellas, deduce el conjunto de etiquetas específicas de versión que debe buscar: cada etiqueta `v{VER}-must-backport` que exista en el repositorio y cuya versión no sea más reciente que la versión activa más reciente. Las etiquetas más antiguas se incluyen incluso cuando su release ya no está activa (se omite una etiqueta más reciente que todas las releases activas, ya que no podría expandirse a ninguna rama activa), por lo que un PR etiquetado para una release en fin de su vida útil sigue encontrándose siempre que haya una versión más reciente activa.

<div id="find-prs-to-backport">
  #### 2. Buscar PR para backport
</div>

`BackportPRs.receive_prs_for_backport` usa la API de búsqueda de GitHub para encontrar PR fusionados que:

* tengan al menos una etiqueta de backport (`pr-must-backport`, `pr-must-backport-force`, `pr-critical-bugfix` o una etiqueta específica de versión), y
* **no** tengan ya `pr-backports-created`, y
* se hayan fusionado después de la fecha del commit más antiguo encontrada en cualquier rama de lanzamiento, y
* se hayan actualizado en los últimos 90 días (para que la consulta de búsqueda siga siendo eficiente).

<div id="rolling-out-branch-handling">
  #### 3. Gestión de ramas `rolling-out`
</div>

Cuando una PR de lanzamiento lleva la etiqueta `rolling-out`, las etiquetas generales de backport (`pr-must-backport`, `pr-critical-bugfix`) omiten esa rama. El bot cierra cualquier PR de cherry-pick o backport creada previamente para esa rama con un comentario explicativo. Una etiqueta específica de versión (p. ej., `v25.3-must-backport`) siempre prevalece sobre esto, tanto para la versión indicada como para cada rama de lanzamiento activa más reciente a la que se expanda. `pr-must-backport-force` omite la comprobación de `rolling-out` para todas las ramas.

<div id="cherry-pick-stage">
  #### 4. Etapa de `cherry-pick` (`ReleaseBranch.create_cherrypick`)
</div>

Para cada par (PR original, rama de lanzamiento) para el que aún no exista un PR de cherry-pick:

1. Haz checkout de la rama de lanzamiento y crea una **rama de backport** (`backport/release/X.Y/N`) a partir de ella.
2. Ejecuta `git merge -s ours` contra el primer parent del commit de merge para crear una base de merge sintética sin cambios de contenido.
3. Crea forzosamente una **rama de cherry-pick** (`cherrypick/release/X.Y/N`) que apunte directamente al commit de merge del PR original.
4. Intenta hacer `git merge --no-commit --no-ff` de la rama de cherry-pick en la rama de backport:
   * Si ya está actualizada, el cambio ya está presente en la rama de lanzamiento; márcalo como completado y omítelo.
   * En caso contrario (con o sin conflictos), restablece y haz push de ambas ramas.
5. Crea el PR de cherry-pick con destino a `backport/release/X.Y/N` desde `cherrypick/release/X.Y/N`, con las etiquetas `pr-cherrypick` y `do not test`.
6. Propaga `pr-bugfix` o `pr-critical-bugfix` desde el PR original, si corresponde.
7. No se asignan assignees en este punto; solo se añaden cuando se detectan conflictos.

<div id="auto-merge-conflict-free-cherry-pick-prs">
  #### 5. Fusión automática de PR de cherry-pick sin conflictos
</div>

Si el PR de cherry-pick se puede fusionar (sin conflictos), el bot lo fusiona automáticamente mediante la API de GitHub y pasa de inmediato a la etapa de backport.

<div id="backport-stage">
  #### 6. Etapa de backport (`ReleaseBranch.create_backport`)
</div>

Después de que se haya fusionado el PR de cherry-pick:

1. Cámbiate a la rama de backport y haz `pull`.
2. Busca el merge-base entre la rama de lanzamiento y la rama de backport.
3. Ejecuta `git reset --soft` hasta el merge-base, compactando todos los commits de cherry-pick en uno solo.
4. Haz commit usando como mensaje el título del PR de backport.
5. Haz force-push de la rama de backport y abre un PR de backport dirigido a la rama de lanzamiento real.
6. Etiqueta el PR con `pr-backport` (y `pr-bugfix` / `pr-critical-bugfix` si corresponde).
7. Asigna el PR al autor del PR original, a quien hizo el merge y a los responsables ya asignados (excluyendo las cuentas de robot).

<div id="completion">
  #### 7. Finalización
</div>

Cuando se ha aplicado backport a todas las ramas de lanzamiento de un PR original determinado, el bot añade `pr-backports-created` al PR original.

<div id="pre-check">
  #### 8. Comprobación previa
</div>

Antes de empezar a trabajar en una PR, `ReleaseBranch.pre_check` ejecuta `git merge-base --is-ancestor` para verificar que el commit de merge no sea ya accesible desde la rama de lanzamiento. Si lo es, la PR se considera ya aplicada mediante backport y se omite.

<div id="stale-cherry-pick-pr-handling">
  ### Gestión de PR de cherry-pick inactivos
</div>

La clase `CherryPickPRs` se ejecuta al inicio de cada ejecución horaria y gestiona dos situaciones:

* **PR de cherry-pick huérfanos**: si la rama de lanzamiento de un PR de cherry-pick ya no tiene un PR de lanzamiento abierto (es decir, si el lanzamiento está cerrado), el PR de cherry-pick se cierra automáticamente.
* **PR de cherry-pick reabiertos**: si un PR original ya tiene `pr-backports-created`, pero un PR de cherry-pick asociado sigue abierto, la etiqueta `pr-backports-created` se elimina del PR original para que pueda volver a procesarse.

Para los PR de cherry-pick que esperan una resolución manual de conflictos:

* Después de **3 días** sin actualizaciones, el bot publica un comentario de recordatorio en el que menciona a las personas asignadas.
* Después de **7 días** sin actualizaciones, el bot publica un comentario de cierre y cierra el PR.

<div id="conflict-resolution">
  ### Resolución de conflictos
</div>

Cuando un cherry-pick tiene conflictos, el PR de cherry-pick se deja abierto para que una persona los resuelva. El bot se lo asigna al autor del PR original, a quien lo fusionó y a sus asignados. Una vez resueltos los conflictos y fusionado el PR de cherry-pick, el bot crea el PR de backport en la siguiente ejecución horaria.

Para descartar por completo un backport, cierra el PR de cherry-pick. El bot lo tratará como omitido intencionadamente.

Para volver a crear desde cero un PR de cherry-pick dañado:

1. Elimina la etiqueta `pr-cherrypick` del PR de cherry-pick.
2. Elimina la rama `cherrypick/...`.
3. Elimina `pr-backports-created` del PR original, si está presente.

<div id="ci-for-backport-prs">
  ### CI para los backport PRs
</div>

Los backport PRs apuntan a ramas de lanzamiento, por lo que usan un workflow de CI específico (`BackportPR`, definido en `ci/workflows/backport_branches.py`) en lugar del workflow estándar de pull request. Este workflow ejecuta un subconjunto representativo de la CI: builds de ASan/UBSan y TSan, builds de release, builds de macOS, pruebas funcionales con ASan, pruebas de estrés con TSan y pruebas de integración. Verifica que la rama de backport tenga entre 1 y 50 commits y al menos un archivo modificado (lo exige `check_backport_branch.py`).

<div id="authentication">
  ### Autenticación
</div>

El flujo de trabajo usa una clave SSH (`ROBOT_CLICKHOUSE_SSH_KEY`) para operaciones de `git push`. Las llamadas a la API de GitHub se autentican mediante `get_best_robot_token`, que selecciona el token con la cuota restante más alta de un grupo almacenado en SSM (`/github-tokens`). `ROBOT_CLICKHOUSE_COMMIT_TOKEN` se usa en el paso de checkout del flujo de trabajo de Actions, no para llamadas a la API. Las cuentas de robot (`robot-clickhouse`, `clickhouse-gh`) se excluyen al asignar a una persona responsable.

<div id="github-api-cache">
  ### Caché de la API de GitHub
</div>

`GitHubCache` (de `cache_utils.py`) guarda de forma persistente la caché de objetos de PyGithub en S3, lo que reduce las llamadas a la API entre ejecuciones horarias. La caché se descarga al inicio y se sube al final de cada ejecución.

<div id="error-handling">
  ### Manejo de errores
</div>

Los errores durante el procesamiento de cada PR se capturan y se registran, pero no detienen la ejecución. Una vez procesados todos los PR, si se produjo algún error, se lanza una `BackportException`. En CI, esto desencadena una notificación a través de `CIBuddy` en el chat del equipo.