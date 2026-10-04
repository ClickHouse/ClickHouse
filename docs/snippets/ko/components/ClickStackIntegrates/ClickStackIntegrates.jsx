export const ClickStackIntegrates = ({ signals = [] }) => {
  const list = Array.isArray(signals) ? signals : String(signals).split(",");
  const supported = list.map((s) => String(s).trim().toLowerCase());

  // 어두운 텍스트를 적용한 ClickHouse 브랜드 노란색(#faff69)입니다.
  // ClickHouseSupportedBadge와 일치하며, 밝은 모드와 어두운 모드에서 동일합니다.
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
      <span style={{ marginRight: "0.5rem" }}>이 가이드에서 통합하는 항목:</span>
      {supported.includes("logs") && <span style={pillStyle}>로그</span>}
      {supported.includes("metrics") && <span style={pillStyle}>메트릭</span>}
      {supported.includes("traces") && <span style={pillStyle}>트레이스</span>}
    </div>
  );
};
export default ClickStackIntegrates;