# Lessons

## Test tag check is mandatory before marking a test done

- Symptom: wrote a test and committed without explicitly checking/applying the tag rule in CLAUDE.md
- Rule: after writing any test, always check CLAUDE.md Test Writing Rules and explicitly decide which tags are needed (or confirm none are needed), and say so proactively — do not wait for the user to ask
- Check: before committing a test file, re-read the "Test Writing Rules" section and verify each rule is satisfied

## Code review: 声称"无代码依赖某字符串"前必须 grep 验证，且要检查 changelog 历史

- Symptom: 审查 `StorageProxy::getName()` 从 `"StorageProxy"` 改为 `"Proxy"` 时，错误地声称"仓库中无代码依赖该字符串"，并认为该名字"从未文档化"，因此给出 Approve 结论。实际上 `tests/queries/0_stateless/00076_system_columns_bytes.sql:16` 明确过滤 `engine != 'StorageProxy'`，且 v24.6 changelog 明确记录了该名字是刻意引入的用户可见行为（PR #64771）。
- Rule: 审查涉及用户可见字符串（engine 名、错误码、system 表列值）的改名时，必须：(1) grep 全仓库字符串字面量；(2) grep `docs/changelogs/` 确认是否有历史记录表明该值已对外承诺。不能仅凭"看起来是内部命名"就跳过这两步。
- Check: `grep -r '"OldName"' src/ tests/ docs/` → `grep -r 'OldName' docs/changelogs/` → 据结果判断是否为 backward-incompatible change

## Code review: 修改模板 Allocator 接口签名时，必须 grep 所有具体实现（含测试 mock）

- Symptom: PR 将 `Memory::dealloc()` 改为 3 参数 `Allocator::free(buf, size, alignment)` 后，漏掉了 `gtest_memory_resize.cpp` 中 `DummyAllocator::free` 只有 2 参数，导致编译失败。
- Rule: 修改 `Allocator` 接口（`alloc`/`free`/`realloc` 签名）后，必须 grep 所有实现了该接口的具体类（含 test mock/stub），确保每一处都与新签名兼容。不能仅检查 production 实现，测试文件里的 fake 同样是接口的实现者。
- Check: `grep -rn "void free(" src/ tests/` → 逐一确认每个 free 实现参数个数与新接口一致

## Code review: 给出结论前必须检查 CI 状态，尤其是 Finish Workflow

- Symptom: 代码修复技术上正确，直接给出 Approve，未检查 CI。实际上 `Finish Workflow` 因 `new_tests_check.py` 失败阻断合并：PR body 含 " Bug Fix" 但无新增测试且无 `s3.amazonaws.com/clickhouse-test-reports` 豁免链接。
- Rule: code review 的最终结论前，必须用 `fetch_ci_report.js` 检查当前 SHA 的 CI 状态（尤其 `Finish Workflow`）。`new_tests_check.py` 对 Bug Fix PR 是强制硬校验，不是"建议"——豁免条件是 PR body 中包含 `s3.amazonaws.com/clickhouse-test-reports` 链接。
- Check: `node .claude/tools/fetch_ci_report.js "<pr_url>" --failed` → 若 `Finish Workflow` 失败，结论为 Block，不得给 Approve

## Code review: 修改共享 helper 时，需分析所有调用方后再判断影响范围

- Symptom: PR 修改了 `wait_loaded_config_changed` 的超时，review 未 grep 全部调用方就下结论；后来过度纠正，认为全局改是 blast radius 过大，实际上逐一分析后发现所有调用方均走相同的 ZK watch 慢路径，全局改是正确的
- Rule: 修改共享 helper 时，必须枚举所有调用路径并分析根因是否适用于每个调用方，再判断全局改还是局部改更合理，不能仅因为影响范围广就反对全局改
- Check: grep 全部调用点 → 对每个调用方逐一判断根因是否相同 → 据此决定修改范围

---

## Code Review: 弱建议不应写成阻塞项

- **Symptom:** 把没有仓库规则依据的建议（tag 注释格式、编号跳跃）写成 Request Changes 的理由
- **Rule:** 每个 blocking finding 必须有明确的仓库规则、测量数据或正确性缺陷作为依据；否则降级为 nit/discussion 或直接删除
- **Check:** 写完 review 后问自己：这条 finding 能引用哪条具体规则或数据？如果答不上来，就不是阻塞项

---

## 2026-04-22: 单元测试的说服力问题

### 症状
创建了单元测试验证 bug，但用户指出"测试没有覆盖真实故障路径，只是弱的 smoke test"。

### 根本原因
1. **过度依赖容错机制**：测试使用了 `ColumnSet::create(1, nullptr)`，触发了 dry_run 模式的容错路径，而不是真实的失败路径
2. **混淆了"测试通过"和"证明 bug"**：测试验证了"最终返回正确结果"，但没有证明"无修复时会产生错误"
3. **声称过强**：将"结构性 smoke test"声称为"直接证明 bug 存在"

### 教训
1. **单元测试的局限性**：
   - 复杂系统中，真实的 bug 场景可能需要大量上下文（如 FutureSetFromSubquery + PreparedSets）
   - 单元测试环境太简化，很难完全模拟真实场景
   - 不要强行用单元测试证明所有 bug

2. **证据的层次**：
   - **代码审查**：适合证明逻辑不一致（如"检查 A 但求值 B"）
   - **集成测试/SQL 测试**：适合端到端验证
   - **单元测试**：适合 API 验证和回归测试
   - 不要用错工具

3. **诚实原则**：
   - 如果测试有局限性，坦诚说明
   - 不要为了"证明观点"而过度声称测试的强度
   - "弱测试"也有价值（回归测试），但要正确定位

### 预防规则
1. **评估测试说服力时问**：
   - 这个测试覆盖了真实的失败路径吗？
   - 测试是否依赖了容错机制（dry_run, try-catch）？
   - 能否区分"有修复"和"无修复"的行为差异？

2. **写测试注释时明确**：
   - 这是 API 测试、回归测试还是 bug 复现？
   - 测试的局限性是什么？
   - 如果依赖容错，明确说明

3. **选择正确的证据**：
   - 代码逻辑问题 → 代码审查
   - 性能问题 → 性能测试/profiling
   - 端到端 bug → 集成测试
   - API 正确性 → 单元测试

### 快速检查
在声称"测试证明了 bug"之前，问：
1. 测试是否真的会触发失败路径？（不是容错路径）
2. 能否通过注释掉修复来让测试失败？
3. 测试的失败模式是否符合 bug 的描述？

如果答案是"不确定"，降低声称强度，改用代码审查作为主要证据。

---

## 2026-04-23: Code review 验证等价性时，必须分析所有中间步骤的副作用

### 症状
声称两种修复方法"对所有场景都功能等价"，忽略了中间步骤 `ActionsDAG::merge` 会调用 `removeUnusedActions`，导致结论错误。

### 根本原因
1. **忽略函数副作用**：只看了"检查 A+B" vs "检查 merge(A,B)"的表面逻辑，没有分析 merge 的实现细节
2. **过度简化证明**：假设"merge 只是合并节点"而没有验证是否会修改/移除节点
3. **测试描述不符**：描述的测试是简化版（Memory 表），实际 PR 使用 MergeTree 表和不同的查询结构

### 教训

1. **验证等价性的步骤**：
   ```
   声称 f(A) ≡ g(A) 前：
   1. 阅读 f 和 g 的源码实现
   2. 识别所有中间步骤（如 merge 调用 removeUnusedActions）
   3. 分析中间步骤是否有副作用（修改输入、移除节点）
   4. 构造反例场景（如"无关节点被移除"）
   5. 只有在所有场景下都验证后，才能声称等价
   ```

2. **关键函数的副作用检查**：
   - `ActionsDAG::merge` → 调用 `removeUnusedActions` → 移除不可达节点
   - `removeUnusedActions` → 从 outputs 反向遍历 → 保留可达，移除不可达
   - 结论：merge 后的 DAG ⊂ merge 前的 DAG（节点数可能减少）

3. **描述测试时的准确性**：
   - 从 PR 的实际 diff 提取测试内容（WebFetch）
   - 不要编造简化版测试（误导读者）
   - 如果测试很长，至少保证表类型、查询结构、关键语句正确

### 预防规则

**声称"方法 A 和方法 B 等价"前，必须**：

1. ✅ **读取源码**：
   ```bash
   # 找到关键函数实现
   grep -n "ActionsDAG::merge" src/Interpreters/ActionsDAG.cpp
   # 阅读完整实现，识别所有子调用
   ```

2. ✅ **识别副作用**：
   - 是否修改输入参数？
   - 是否调用其他修改状态的函数？
   - 是否有条件分支导致不同行为？

3. ✅ **构造反例**：
   - 尝试找一个场景使 A 和 B 行为不同
   - 边缘场景：空输入、无关节点、重复节点

4. ✅ **限定范围**：
   - 如果只在特定场景下等价，明确说明："在 X 场景下等价"
   - 如果有反例，降级为："不严格等价，但在核心场景上等价"

**描述测试时**：

1. ✅ **从源获取**：
   ```bash
   # 使用 WebFetch 获取实际测试文件
   WebFetch("https://github.com/.../pull/.../files")
   ```

2. ✅ **核对关键信息**：
   - 表 ENGINE（MergeTree vs Memory）
   - 查询结构（JOIN 类型、WHERE 条件）
   - 关键语句（TRUNCATE、设置）

3. ✅ **标注简化**：
   - 如果简化，明确说明："以下是简化版，实际测试使用 X"
   - 如果长度限制，提取关键部分："完整测试见文件，关键部分如下"

### 快速检查

在声称"A 等价于 B"之前：

1. **是否读了 A 和 B 的完整实现**？
   - 如果只看了函数签名 → ❌ 不足

2. **是否分析了所有中间步骤**？
   - 如果只看了顶层逻辑 → ❌ 不足
   - 需要追踪所有子函数调用

3. **是否尝试构造反例**？
   - 如果只想"应该等价吧" → ❌ 不足
   - 需要主动寻找不等价的场景

4. **结论是否限定范围**？
   - "对所有场景都等价" → ⚠️ 需要强证据
   - "在 X 场景下等价" → ✅ 更安全

在描述测试时：

1. **是否从实际 diff 提取**？
   - 如果是凭印象编写 → ❌ 可能错误

2. **关键信息是否核对**？
   - 表类型、查询结构、关键语句

### 本次错误的修正

错误文档：`/home/shaohua/work/t-claude/code_review/103029/pr_fix_verification.md`

修正文档：`/home/shaohua/work/t-claude/code_review/103029/pr_fix_verification_corrected.md`

关键修正：
1. ❌ "两种方法对所有场景都功能等价" → ✅ "两种方法在核心场景上等价，在边缘场景不等价"
2. ❌ "#103029 性能更优" → ✅ "两种方法各有优缺点：#103029 更保守，#103129 更精确"
3. ❌ 简化的 Memory 表测试 → ✅ 实际的 MergeTree 表测试

---

## 2026-04-23: 区分"异常路径"和"服务器崩溃"，不夸大测试证明的内容

### 症状
文档多处声称"真实 FunctionIn 会崩溃"，但实际上生产代码有 try-catch 捕获异常并返回 UNKNOWN。死亡测试绕过了真实函数的 try-catch，只证明了底层代码路径会抛异常，不能证明真实函数会崩溃。

### 根本原因
1. **混淆了两个不同的代码路径**：
   - 真实的 `filterResultForMatchedRows`（有 try-catch）
   - 测试的 `evaluateMatchedRowsNoCombinedCheckNoCatch`（无 try-catch）
2. **夸大了失败模式的严重性**：
   - 实际：不必要的异常路径（被 try-catch 捕获）
   - 声称：服务器崩溃（进程终止）
3. **过度解读测试证明的内容**：
   - 测试证明：底层代码路径会抛异常
   - 声称：真实函数会崩溃

### 教训

1. **区分不同严重级别的问题**：
   - **服务器崩溃**：进程终止，用户可见
   - **异常路径**：抛异常但被捕获，用户不可见
   - **设计缺陷**：依赖异常处理作为控制流
   - 三者严重性完全不同，不能混为一谈

2. **准确描述测试证明的内容**：
   - 如果测试绕过了真实函数（如用辅助函数去掉 try-catch）
   - 只能说"测试证明底层代码路径会抛异常"
   - 不能说"测试证明真实函数会崩溃"
   - 测试的人工环境 ≠ 生产环境

3. **检查生产代码的保护机制**：
   - 在声称"会崩溃"前，检查是否有 try-catch
   - 在声称"会失败"前，检查是否有错误处理
   - 在声称"会超时"前，检查是否有超时保护

4. **修复价值的正确定位**：
   - 即使不会崩溃，修复仍可能必要：
     - 逻辑正确性（检查对象应覆盖求值对象）
     - 性能优化（避免不必要的异常）
     - 代码清晰度（不依赖异常作为控制流）
   - 但要准确描述修复解决的问题等级

### 预防规则

**声称"会崩溃/失败"前的检查清单**：

1. ✅ **检查真实代码路径**：
   - 读取实际的生产代码
   - 确认是否有 try-catch, 错误处理, 超时保护
   - 区分"会抛异常"和"会崩溃"

2. ✅ **检查测试环境**：
   - 测试是否使用真实函数？还是辅助函数？
   - 测试是否绕过了保护机制（如去掉 try-catch）？
   - 测试的人工环境和生产环境是否一致？

3. ✅ **准确定位问题严重性**：
   ```
   用户可见崩溃 > 隐藏的异常路径 > 设计缺陷 > 性能问题
   ```

4. ✅ **准确描述测试证明的内容**：
   - "测试证明 X"必须是测试直接验证的
   - 如果测试绕过了真实路径，说明"测试证明底层会 Y"
   - 不要从测试结果过度推断生产行为

### 快速检查

在写"会崩溃"前，问：
1. **生产代码有 try-catch 吗**？
   - 如果有 → 不会崩溃，改为"会抛异常被捕获"

2. **测试用的是真实函数吗**？
   - 如果不是（如用辅助函数绕过）→ 说明"测试证明底层路径会 X"

3. **用户会看到失败吗**？
   - 如果不会 → 这是设计缺陷，不是崩溃

### 本次错误的修正

**错误版本**：`/home/shaohua/work/t-claude/code_review/103029/final_decisive_proof.md`
- 多处声称"真实 FunctionIn 会崩溃"
- 声称"决定性证据"
- 声称"从理论问题升级到实际崩溃"

**修正版本**：`/home/shaohua/work/t-claude/code_review/103029/final_decisive_proof_corrected.md`
- 改为"底层代码路径会抛异常（被 try-catch 捕获）"
- 改为"不必要的异常路径证据"
- 定位为"设计缺陷：依赖异常处理作为控制流"
- 明确说明"不是用户可见的服务器崩溃"

关键修正：
1. 删除所有"崩溃"声称 → 改为"抛异常"
2. 删除"决定性证据" → 改为"设计缺陷证据"
3. 明确测试绕过了真实函数的 try-catch
4. 准确定位修复价值：逻辑、性能、清晰度，而非防止崩溃

