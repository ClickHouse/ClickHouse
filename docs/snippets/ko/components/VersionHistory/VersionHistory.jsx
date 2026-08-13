const VersionHistory = ({ rows = [] }) => {
  const headers = ["버전", "기본값", "설명"];
  return (
    <Accordion title="버전 이력">
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