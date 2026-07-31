const VersionHistory = ({ rows = [] }) => {
  const headers = ["الإصدار", "القيمة الافتراضية", "ملاحظة"];
  return (
    <Accordion title="سجل الإصدارات">
      <table>
        <thead>
          <tr>
            {headers.map((h) => (
              <th key={h}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={row.id ?? ri}>
              {(row.items ?? []).map((cell, ci) => (
                <td key={ci}>{cell?.label}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </Accordion>
  );
};
export default VersionHistory;