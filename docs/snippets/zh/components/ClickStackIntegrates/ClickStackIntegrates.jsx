export const ClickStackIntegrates = ({ signals = [] }) => {
  const list = Array.isArray(signals) ? signals : String(signals).split(",");
  const supported = list.map((s) => String(s).trim().toLowerCase());

  // 使用 ClickHouse 品牌黄色 (#faff69) 和深色文本，与
  // ClickHouseSupportedBadge 保持一致；浅色和深色模式下均相同。
  const pillStyle = {
    display: "inline-flex",
    alignItems: "center",
    padding: "0.125rem 0.625rem",
    borderRadius: "9999px",
    fontSize: "0.8125rem",
    fontWeight: 500,
    lineHeight: 1.6,
    color: "#161517",
    backgroundColor: "#faff69",
    border: "1px solid rgba(0, 0, 0, .1)",
    marginRight: "0.5rem",
  };

  return (
    <div style={{ margin: "0 0 1.25rem" }}>
      <span style={{ marginRight: "0.5rem" }}>本指南集成了：</span>
      {supported.includes("logs") && <span style={pillStyle}>日志</span>}
      {supported.includes("metrics") && <span style={pillStyle}>指标</span>}
      {supported.includes("traces") && <span style={pillStyle}>链路追踪</span>}
    </div>
  );
};
export default ClickStackIntegrates;