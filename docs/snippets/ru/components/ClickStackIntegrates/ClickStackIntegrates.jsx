export const ClickStackIntegrates = ({ signals = [] }) => {
  const list = Array.isArray(signals) ? signals : String(signals).split(",");
  const supported = list.map((s) => String(s).trim().toLowerCase());

  // Фирменный жёлтый цвет ClickHouse (#faff69) с тёмным текстом,
  // соответствует ClickHouseSupportedBadge; одинаков в светлой и тёмной темах.
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
      <span style={{ marginRight: "0.5rem" }}>Это руководство поддерживает:</span>
      {supported.includes("logs") && <span style={pillStyle}>Журналы</span>}
      {supported.includes("metrics") && <span style={pillStyle}>Метрики</span>}
      {supported.includes("traces") && <span style={pillStyle}>Трассировки</span>}
    </div>
  );
};
export default ClickStackIntegrates;